/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#if defined(__APPLE__)
# if ! defined(__DARWIN_64_BIT_INO_T) && ! defined(_DARWIN_USE_64_BIT_INODE)
#  define __DARWIN_64_BIT_INO_T 0
# endif
#endif

#include "config.h"
#include "mount/fs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/access_control_list.h"
#include "common/acl_converter.h"
#include "common/acl_type.h"
#include "common/datapack.h"
#include "common/MFSCommunication.h"
#include "common/posix_acl_xattr.h"
#include "common/strerr.h"
#include "common/time_utils.h"
#include "mount/dirattrcache.h"
#include "mount/g_io_limiters.h"
#include "mount/io_limit_group.h"
#include "mount/mastercomm.h"
#include "mount/masterproxy.h"
#include "mount/oplog.h"
#include "mount/readdata.h"
#include "mount/stats.h"
#include "mount/symlinkcache.h"
#include "mount/writedata.h"

#define READDIR_BUFFSIZE 50000

#define MAX_FILE_SIZE (int64_t)(MFS_MAX_FILE_SIZE)

#define PKGVERSION ((PACKAGE_VERSION_MAJOR)*1000000+(PACKAGE_VERSION_MINOR)*1000+(PACKAGE_VERSION_MICRO))

// #define MASTER_NAME ".master"
// #define MASTER_INODE 0x7FFFFFFF
// 0x01b6 == 0666
// static uint8_t masterattr[35]={'f', 0x01,0xB6, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define MASTERINFO_NAME ".masterinfo"
#define MASTERINFO_INODE 0x7FFFFFFF
// 0x0124 == 0b100100100 == 0444
#ifdef MASTERINFO_WITH_VERSION
static uint8_t masterinfoattr[35]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,14};
#else
static uint8_t masterinfoattr[35]={'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,10};
#endif

#define STATS_NAME ".stats"
#define STATS_INODE 0x7FFFFFF0
// 0x01A4 == 0b110100100 == 0644
static uint8_t statsattr[35]={'f', 0x01,0xA4, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define OPLOG_NAME ".oplog"
#define OPLOG_INODE 0x7FFFFFF1
#define OPHISTORY_NAME ".ophistory"
#define OPHISTORY_INODE 0x7FFFFFF2
// 0x0100 == 0b100000000 == 0400
static uint8_t oplogattr[35]={'f', 0x01,0x00, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0};

#define MIN_SPECIAL_INODE 0x7FFFFFF0
#define IS_SPECIAL_INODE(ino) ((ino)>=MIN_SPECIAL_INODE)
#define IS_SPECIAL_NAME(name) ((name)[0]=='.' && (strcmp(STATS_NAME,(name))==0 || strcmp(MASTERINFO_NAME,(name))==0 || strcmp(OPLOG_NAME,(name))==0 || strcmp(OPHISTORY_NAME,(name))==0/* || strcmp(ATTRCACHE_NAME,(name))==0*/))

typedef struct _sinfo {
	char *buff;
	uint32_t leng;
	uint8_t reset;
	pthread_mutex_t lock;
} sinfo;

typedef struct _dirbuf {
	int wasread;
	int dataformat;
	uid_t uid;
	gid_t gid;
	const uint8_t *p;
	size_t size;
	void *dcache;
	pthread_mutex_t lock;
} dirbuf;

enum {IO_NONE,IO_READ,IO_WRITE,IO_READONLY,IO_WRITEONLY};

typedef struct _finfo {
	uint8_t mode;
	void *data;
	pthread_mutex_t lock;
} finfo;

static int debug_mode = 0;
static int usedircache = 1;
static int keep_cache = 0;
static double direntry_cache_timeout = 0.1;
static double entry_cache_timeout = 0.0;
static double attr_cache_timeout = 0.1;
static int mkdir_copy_sgid = 0;
static int sugid_clear_mode = 0;
static bool acl_enabled = 0;

enum {
	OP_STATFS = 0,
	OP_ACCESS,
	OP_LOOKUP,
	OP_LOOKUP_INTERNAL,
	OP_DIRCACHE_LOOKUP,
	OP_GETATTR,
	OP_DIRCACHE_GETATTR,
	OP_SETATTR,
	OP_MKNOD,
	OP_UNLINK,
	OP_MKDIR,
	OP_RMDIR,
	OP_SYMLINK,
	OP_READLINK,
	OP_READLINK_CACHED,
	OP_RENAME,
	OP_LINK,
	OP_OPENDIR,
	OP_READDIR,
	OP_RELEASEDIR,
	OP_CREATE,
	OP_OPEN,
	OP_RELEASE,
	OP_READ,
	OP_WRITE,
	OP_FLUSH,
	OP_FSYNC,
	OP_SETXATTR,
	OP_GETXATTR,
	OP_LISTXATTR,
	OP_REMOVEXATTR,
	OP_GETDIR_FULL,
	OP_GETDIR_SMALL,
	STATNODES
};

static uint64_t *statsptr[STATNODES];

void fs_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"fuse_ops",0);
	statsptr[OP_SETXATTR] = stats_get_counterptr(stats_get_subnode(s,"setxattr",0));
	statsptr[OP_GETXATTR] = stats_get_counterptr(stats_get_subnode(s,"getxattr",0));
	statsptr[OP_LISTXATTR] = stats_get_counterptr(stats_get_subnode(s,"listxattr",0));
	statsptr[OP_REMOVEXATTR] = stats_get_counterptr(stats_get_subnode(s,"removexattr",0));
	statsptr[OP_FSYNC] = stats_get_counterptr(stats_get_subnode(s,"fsync",0));
	statsptr[OP_FLUSH] = stats_get_counterptr(stats_get_subnode(s,"flush",0));
	statsptr[OP_WRITE] = stats_get_counterptr(stats_get_subnode(s,"write",0));
	statsptr[OP_READ] = stats_get_counterptr(stats_get_subnode(s,"read",0));
	statsptr[OP_RELEASE] = stats_get_counterptr(stats_get_subnode(s,"release",0));
	statsptr[OP_OPEN] = stats_get_counterptr(stats_get_subnode(s,"open",0));
	statsptr[OP_CREATE] = stats_get_counterptr(stats_get_subnode(s,"create",0));
	statsptr[OP_RELEASEDIR] = stats_get_counterptr(stats_get_subnode(s,"releasedir",0));
	statsptr[OP_READDIR] = stats_get_counterptr(stats_get_subnode(s,"readdir",0));
	statsptr[OP_OPENDIR] = stats_get_counterptr(stats_get_subnode(s,"opendir",0));
	statsptr[OP_LINK] = stats_get_counterptr(stats_get_subnode(s,"link",0));
	statsptr[OP_RENAME] = stats_get_counterptr(stats_get_subnode(s,"rename",0));
	statsptr[OP_READLINK] = stats_get_counterptr(stats_get_subnode(s,"readlink",0));
	statsptr[OP_READLINK_CACHED] = stats_get_counterptr(stats_get_subnode(s,"readlink-cached",0));
	statsptr[OP_SYMLINK] = stats_get_counterptr(stats_get_subnode(s,"symlink",0));
	statsptr[OP_RMDIR] = stats_get_counterptr(stats_get_subnode(s,"rmdir",0));
	statsptr[OP_MKDIR] = stats_get_counterptr(stats_get_subnode(s,"mkdir",0));
	statsptr[OP_UNLINK] = stats_get_counterptr(stats_get_subnode(s,"unlink",0));
	statsptr[OP_MKNOD] = stats_get_counterptr(stats_get_subnode(s,"mknod",0));
	statsptr[OP_SETATTR] = stats_get_counterptr(stats_get_subnode(s,"setattr",0));
	statsptr[OP_GETATTR] = stats_get_counterptr(stats_get_subnode(s,"getattr",0));
	statsptr[OP_DIRCACHE_GETATTR] = stats_get_counterptr(stats_get_subnode(s,"getattr-cached",0));
	statsptr[OP_LOOKUP] = stats_get_counterptr(stats_get_subnode(s,"lookup",0));
	statsptr[OP_LOOKUP_INTERNAL] = stats_get_counterptr(stats_get_subnode(s,"lookup-internal",0));
	if (usedircache) {
		statsptr[OP_DIRCACHE_LOOKUP] = stats_get_counterptr(stats_get_subnode(s,"lookup-cached",0));
	}
	statsptr[OP_ACCESS] = stats_get_counterptr(stats_get_subnode(s,"access",0));
	statsptr[OP_STATFS] = stats_get_counterptr(stats_get_subnode(s,"statfs",0));
	if (usedircache) {
		statsptr[OP_GETDIR_FULL] = stats_get_counterptr(stats_get_subnode(s,"getdir-full",0));
	} else {
		statsptr[OP_GETDIR_SMALL] = stats_get_counterptr(stats_get_subnode(s,"getdir-small",0));
	}
}

static void mfs_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		stats_lock();
		(*statsptr[id])++;
		stats_unlock();
	}
}

#ifndef EDQUOT
# define EDQUOT ENOSPC
#endif
#ifndef ENOATTR
# ifdef ENODATA
#  define ENOATTR ENODATA
# else
#  define ENOATTR ENOENT
# endif
#endif

static int mfs_errorconv(int status) {
	int ret;
	switch (status) {
	case STATUS_OK:
		ret=0;
		break;
	case ERROR_EPERM:
		ret=EPERM;
		break;
	case ERROR_ENOTDIR:
		ret=ENOTDIR;
		break;
	case ERROR_ENOENT:
		ret=ENOENT;
		break;
	case ERROR_EACCES:
		ret=EACCES;
		break;
	case ERROR_EEXIST:
		ret=EEXIST;
		break;
	case ERROR_EINVAL:
		ret=EINVAL;
		break;
	case ERROR_ENOTEMPTY:
		ret=ENOTEMPTY;
		break;
	case ERROR_IO:
		ret=EIO;
		break;
	case ERROR_EROFS:
		ret=EROFS;
		break;
	case ERROR_QUOTA:
		ret=EDQUOT;
		break;
	case ERROR_ENOATTR:
		ret=ENOATTR;
		break;
	case ERROR_ENOTSUP:
		ret=ENOTSUP;
		break;
	case ERROR_ERANGE:
		ret=ERANGE;
		break;
	default:
		ret=EINVAL;
		break;
	}
	if (debug_mode && ret!=0) {
		fprintf(stderr, "status: %s\n", strerr(ret));
	}
	return ret;
}

static void mfs_type_to_stat(uint32_t inode,uint8_t type, struct stat *stbuf) {
	memset(stbuf,0,sizeof(struct stat));
	stbuf->st_ino = inode;
	switch (type) {
	case TYPE_DIRECTORY:
		stbuf->st_mode = S_IFDIR;
		break;
	case TYPE_SYMLINK:
		stbuf->st_mode = S_IFLNK;
		break;
	case TYPE_FILE:
		stbuf->st_mode = S_IFREG;
		break;
	case TYPE_FIFO:
		stbuf->st_mode = S_IFIFO;
		break;
	case TYPE_SOCKET:
		stbuf->st_mode = S_IFSOCK;
		break;
	case TYPE_BLOCKDEV:
		stbuf->st_mode = S_IFBLK;
		break;
	case TYPE_CHARDEV:
		stbuf->st_mode = S_IFCHR;
		break;
	default:
		stbuf->st_mode = 0;
	}
}

static uint8_t mfs_attr_get_mattr(const uint8_t attr[35]) {
	return (attr[1]>>4);    // higher 4 bits of mode
}

static void mfs_attr_to_stat(uint32_t inode,const uint8_t attr[35], struct stat *stbuf) {
	uint16_t attrmode;
	uint8_t attrtype;
	uint32_t attruid,attrgid,attratime,attrmtime,attrctime,attrnlink,attrrdev;
	uint64_t attrlength;
	const uint8_t *ptr;
	ptr = attr;
	attrtype = get8bit(&ptr);
	attrmode = get16bit(&ptr);
	attruid = get32bit(&ptr);
	attrgid = get32bit(&ptr);
	attratime = get32bit(&ptr);
	attrmtime = get32bit(&ptr);
	attrctime = get32bit(&ptr);
	attrnlink = get32bit(&ptr);
	stbuf->st_ino = inode;
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	stbuf->st_blksize = MFSBLOCKSIZE;
#endif
	switch (attrtype) {
	case TYPE_DIRECTORY:
		stbuf->st_mode = S_IFDIR | (attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_SYMLINK:
		stbuf->st_mode = S_IFLNK | (attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_FILE:
		stbuf->st_mode = S_IFREG | (attrmode & 07777);
		attrlength = get64bit(&ptr);
		stbuf->st_size = attrlength;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = (attrlength+511)/512;
#endif
		break;
	case TYPE_FIFO:
		stbuf->st_mode = S_IFIFO | (attrmode & 07777);
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_SOCKET:
		stbuf->st_mode = S_IFSOCK | (attrmode & 07777);
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_BLOCKDEV:
		stbuf->st_mode = S_IFBLK | (attrmode & 07777);
		attrrdev = get32bit(&ptr);
#ifdef HAVE_STRUCT_STAT_ST_RDEV
		stbuf->st_rdev = attrrdev;
#endif
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	case TYPE_CHARDEV:
		stbuf->st_mode = S_IFCHR | (attrmode & 07777);
		attrrdev = get32bit(&ptr);
#ifdef HAVE_STRUCT_STAT_ST_RDEV
		stbuf->st_rdev = attrrdev;
#endif
		stbuf->st_size = 0;
#ifdef HAVE_STRUCT_STAT_ST_BLOCKS
		stbuf->st_blocks = 0;
#endif
		break;
	default:
		stbuf->st_mode = 0;
	}
	stbuf->st_uid = attruid;
	stbuf->st_gid = attrgid;
	stbuf->st_atime = attratime;
	stbuf->st_mtime = attrmtime;
	stbuf->st_ctime = attrctime;
#ifdef HAVE_STRUCT_STAT_ST_BIRTHTIME
	stbuf->st_birthtime = attrctime;        // for future use
#endif
	stbuf->st_nlink = attrnlink;
}

static inline void mfs_makemodestr(char modestr[11],uint16_t mode) {
	uint32_t i;
	strcpy(modestr,"?rwxrwxrwx");
	switch (mode & S_IFMT) {
	case S_IFSOCK:
		modestr[0] = 's';
		break;
	case S_IFLNK:
		modestr[0] = 'l';
		break;
	case S_IFREG:
		modestr[0] = '-';
		break;
	case S_IFBLK:
		modestr[0] = 'b';
		break;
	case S_IFDIR:
		modestr[0] = 'd';
		break;
	case S_IFCHR:
		modestr[0] = 'c';
		break;
	case S_IFIFO:
		modestr[0] = 'f';
		break;
	}
	if (mode & S_ISUID) {
		modestr[3] = 's';
	}
	if (mode & S_ISGID) {
		modestr[6] = 's';
	}
	if (mode & S_ISVTX) {
		modestr[9] = 't';
	}
	for (i=0 ; i<9 ; i++) {
		if ((mode & (1<<i))==0) {
			if (modestr[9-i]=='s' || modestr[9-i]=='t') {
				modestr[9-i]&=0xDF;
			} else {
				modestr[9-i]='-';
			}
		}
	}
}

static void mfs_makeattrstr(char *buff,uint32_t size,struct stat *stbuf) {
	char modestr[11];
	mfs_makemodestr(modestr,stbuf->st_mode);
#ifdef HAVE_STRUCT_STAT_ST_RDEV
	if (modestr[0]=='b' || modestr[0]=='c') {
		snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%" PRIu64 ",%08lX]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(uint64_t)(stbuf->st_size),(unsigned long int)(stbuf->st_rdev));
	} else {
		snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%" PRIu64 "]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(uint64_t)(stbuf->st_size));
	}
#else
	snprintf(buff,size,"[%s:0%06o,%u,%ld,%ld,%lu,%lu,%lu,%" PRIu64 "]",modestr,(unsigned int)(stbuf->st_mode),(unsigned int)(stbuf->st_nlink),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(unsigned long int)(stbuf->st_ctime),(uint64_t)(stbuf->st_size));
#endif
}


// FIXME provide proper class
// TODO check if aaa != 0
#define FsException(aaa) aaa


//// TODO move up to wrapper
//#if FUSE_USE_VERSION >= 26
//void mfs_statfs(fuse_req_t req,inode_t ino) {
//#else
//void mfs_statfs(fuse_req_t req) {
//#endif

struct statvfs fs::statfs(fs_ctx ctx, inode_t ino) {
	uint64_t totalspace,availspace,trashspace,reservedspace;
	uint32_t inodes;
	uint32_t bsize;
	struct statvfs stfsbuf;
	memset(&stfsbuf,0,sizeof(stfsbuf));

	mfs_stats_inc(OP_STATFS);
	if (debug_mode) {
		oplog_printf(ctx, "statfs (%lu)", (unsigned long int)ino);
	}
	(void)ino;
	fs_statfs(&totalspace,&availspace,&trashspace,&reservedspace,&inodes);

#if defined(__APPLE__)
	if (totalspace>0x0001000000000000ULL) {
		bsize = 0x20000;
	} else {
		bsize = 0x10000;
	}
#else
	bsize = 0x10000;
#endif

	stfsbuf.f_namemax = MFS_NAME_MAX;
	stfsbuf.f_frsize = bsize;
	stfsbuf.f_bsize = bsize;
#if defined(__APPLE__)
	// FUSE on apple (or other parts of kernel) expects 32-bit values, so it's better to saturate this values than let being cut on 32-bit
	// can't change bsize also because 64k seems to be the biggest acceptable value for bsize

	if (totalspace/bsize>0xFFFFFFFFU) {
		stfsbuf.f_blocks = 0xFFFFFFFFU;
	} else {
		stfsbuf.f_blocks = totalspace/bsize;
	}
	if (availspace/bsize>0xFFFFFFFFU) {
		stfsbuf.f_bfree = 0xFFFFFFFFU;
		stfsbuf.f_bavail = 0xFFFFFFFFU;
	} else {
		stfsbuf.f_bfree = availspace/bsize;
		stfsbuf.f_bavail = availspace/bsize;
	}
#else
	stfsbuf.f_blocks = totalspace/bsize;
	stfsbuf.f_bfree = availspace/bsize;
	stfsbuf.f_bavail = availspace/bsize;
#endif
	stfsbuf.f_files = 1000000000+PKGVERSION+inodes;
	stfsbuf.f_ffree = 1000000000+PKGVERSION;
	stfsbuf.f_favail = 1000000000+PKGVERSION;
	//stfsbuf.f_flag = ST_RDONLY;
	oplog_printf(ctx,
			"statfs (%lu): OK (%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu32 ")",
			(unsigned long int)ino, totalspace, availspace, trashspace, reservedspace, inodes);
	return stfsbuf;
}

void fs::access(fs_ctx ctx, inode_t ino, int mask) {
	int status;

	int mmode;

	oplog_printf(ctx,"access (%lu,0x%X)",(unsigned long int)ino,mask);
	mfs_stats_inc(OP_ACCESS);
#if (R_OK==MODE_MASK_R) && (W_OK==MODE_MASK_W) && (X_OK==MODE_MASK_X)
	mmode = mask & (MODE_MASK_R | MODE_MASK_W | MODE_MASK_X);
#else
	mmode = 0;
	if (mask & R_OK) {
		mmode |= MODE_MASK_R;
	}
	if (mask & W_OK) {
		mmode |= MODE_MASK_W;
	}
	if (mask & X_OK) {
		mmode |= MODE_MASK_X;
	}
#endif
	if (IS_SPECIAL_INODE(ino)) {
		if (mask & (W_OK | X_OK)) {
			throw FsException(EACCES);
		}
		return;
	}
	status = fs_access(ino,ctx.uid,ctx.gid,mmode);
	status = mfs_errorconv(status);
	if (status!=0) {
		throw FsException(status);
	}
}

FsEntryParam fs::lookup(fs_ctx ctx, inode_t parent, const char *name) {
	FsEntryParam e;
	uint64_t maxfleng;
	uint32_t inode;
	uint32_t nleng;
	uint8_t attr[35];
	char attrstr[256];
	uint8_t mattr;
	uint8_t icacheflag;
	int status;

	if (debug_mode) {
		oplog_printf(ctx,"lookup (%lu,%s) ...",(unsigned long int)parent,name);
		fprintf(stderr,"lookup (%lu,%s)\n",(unsigned long int)parent,name);
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		mfs_stats_inc(OP_LOOKUP);
		oplog_printf(ctx,"lookup (%lu,%s): %s",(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}
	if (parent==MFS_ROOT_ID) {
		if (nleng==2 && name[0]=='.' && name[1]=='.') {
			nleng=1;
		}
		if (strcmp(name,MASTERINFO_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = MASTERINFO_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(MASTERINFO_INODE,masterinfoattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(ctx,"lookup (%lu,%s) (internal node: MASTERINFO): OK (%.1f,%lu,%.1f,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			return e;
		}
		if (strcmp(name,STATS_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = STATS_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(STATS_INODE,statsattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(ctx,"lookup (%lu,%s) (internal node: STATS): OK (%.1f,%lu,%.1f,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			return e;
		}
		if (strcmp(name,OPLOG_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = OPLOG_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(OPLOG_INODE,oplogattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(ctx,"lookup (%lu,%s) (internal node: OPLOG): OK (%.1f,%lu,%.1f,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			return e;
		}
		if (strcmp(name,OPHISTORY_NAME)==0) {
			memset(&e, 0, sizeof(e));
			e.ino = OPHISTORY_INODE;
			e.attr_timeout = 3600.0;
			e.entry_timeout = 3600.0;
			mfs_attr_to_stat(OPHISTORY_INODE,oplogattr,&e.attr);
			mfs_stats_inc(OP_LOOKUP_INTERNAL);
			mfs_makeattrstr(attrstr,256,&e.attr);
			oplog_printf(ctx,"lookup (%lu,%s) (internal node: OPHISTORY): OK (%.1f,%lu,%.1f,%s)",(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
			return e;
		}
	}
	if (usedircache && dcache_lookup(&ctx,parent,nleng,(const uint8_t*)name,&inode,attr)) {
		if (debug_mode) {
			fprintf(stderr,"lookup: sending data from dircache\n");
		}
		mfs_stats_inc(OP_DIRCACHE_LOOKUP);
		status = 0;
		icacheflag = 1;
//              oplog_printf(ctx,"lookup (%lu,%s) (using open dir cache): OK (%lu)",(unsigned long int)parent,name,(unsigned long int)inode);
	} else {
		mfs_stats_inc(OP_LOOKUP);
		status = fs_lookup(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid,&inode,attr);
		status = mfs_errorconv(status);
		icacheflag = 0;
	}
	if (status!=0) {
		oplog_printf(ctx,"lookup (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
		throw FsException(status);
	}
	if (attr[0]==TYPE_FILE) {
		maxfleng = write_data_getmaxfleng(inode);
	} else {
		maxfleng = 0;
	}
	memset(&e, 0, sizeof(e));
	e.ino = inode;
	mattr = mfs_attr_get_mattr(attr);
	e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:((attr[0]==TYPE_DIRECTORY)?direntry_cache_timeout:entry_cache_timeout);
	mfs_attr_to_stat(inode,attr,&e.attr);
	if (maxfleng>(uint64_t)(e.attr.st_size)) {
		e.attr.st_size=maxfleng;
	}
	mfs_makeattrstr(attrstr,256,&e.attr);
	oplog_printf(ctx,"lookup (%lu,%s)%s: OK (%.1f,%lu,%.1f,%s)",(unsigned long int)parent,name,icacheflag?" (using open dir cache)":"",e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
	return e;
}

fs::AttrReply fs::getattr(fs_ctx ctx, inode_t ino, fs_file_info* fi) {
	uint64_t maxfleng;
	double attr_timeout;
	struct stat o_stbuf;
	uint8_t attr[35];
	char attrstr[256];
	int status;
	(void)fi;

	if (debug_mode) {
		oplog_printf(ctx,"getattr (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"getattr (%lu)\n",(unsigned long int)ino);
	}
	if (ino==MASTERINFO_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,masterinfoattr,&o_stbuf);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"getattr (%lu) (internal node: MASTERINFO): OK (3600,%s)",(unsigned long int)ino,attrstr);
		return fs::AttrReply{o_stbuf, 3600.0};
	}
	if (ino==STATS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,statsattr,&o_stbuf);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"getattr (%lu) (internal node: STATS): OK (3600,%s)",(unsigned long int)ino,attrstr);
		return fs::AttrReply{o_stbuf, 3600.0};
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,oplogattr,&o_stbuf);
		mfs_stats_inc(OP_GETATTR);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"getattr (%lu) (internal node: %s): OK (3600,%s)",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",attrstr);
		return fs::AttrReply{o_stbuf, 3600.0};
	}
	if (usedircache && dcache_getattr(&ctx,ino,attr)) {
		if (debug_mode) {
			fprintf(stderr,"getattr: sending data from dircache\n");
		}
		mfs_stats_inc(OP_DIRCACHE_GETATTR);
		status = 0;
	} else {
		mfs_stats_inc(OP_GETATTR);
		status = fs_getattr(ino,ctx.uid,ctx.gid,attr);
		status = mfs_errorconv(status);
	}
	if (status!=0) {
		oplog_printf(ctx,"getattr (%lu): %s",(unsigned long int)ino,strerr(status));
		throw FsException(status);
	}
	if (attr[0]==TYPE_FILE) {
		maxfleng = write_data_getmaxfleng(ino);
	} else {
		maxfleng = 0;
	}
	memset(&o_stbuf, 0, sizeof(struct stat));
	mfs_attr_to_stat(ino,attr,&o_stbuf);
	if (maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
	}
	attr_timeout = (mfs_attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	mfs_makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(ctx,"getattr (%lu): OK (%.1f,%s)",(unsigned long int)ino,attr_timeout,attrstr);
	return fs::AttrReply{o_stbuf, attr_timeout};
}

// TODO consider making oplog_printf asynchronous

// TODO blee
#define FUSE_SET_ATTR_MODE	(1 << 0)
#define FUSE_SET_ATTR_UID	(1 << 1)
#define FUSE_SET_ATTR_GID	(1 << 2)
#define FUSE_SET_ATTR_SIZE	(1 << 3)
#define FUSE_SET_ATTR_ATIME	(1 << 4)
#define FUSE_SET_ATTR_MTIME	(1 << 5)
#define FUSE_SET_ATTR_ATIME_NOW	(1 << 7)
#define FUSE_SET_ATTR_MTIME_NOW	(1 << 8)

fs::AttrReply fs::setattr(fs_ctx ctx, inode_t ino, struct stat *stbuf,
		int to_set, fs_file_info* fi) {
	struct stat o_stbuf;
	uint64_t maxfleng;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	double attr_timeout;
	int status;

	mfs_makemodestr(modestr,stbuf->st_mode);
	mfs_stats_inc(OP_SETATTR);
	if (debug_mode) {
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]) ...",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size));
		fprintf(stderr,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "])\n",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size));
	}
	if (ino==MASTERINFO_INODE) {
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),strerr(EPERM));
		throw FsException(EPERM);
	}
	if (ino==STATS_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,statsattr,&o_stbuf);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]) (internal node: STATS): OK (3600,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),attrstr);
		return fs::AttrReply{o_stbuf, 3600.0};
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		memset(&o_stbuf, 0, sizeof(struct stat));
		mfs_attr_to_stat(ino,oplogattr,&o_stbuf);
		mfs_makeattrstr(attrstr,256,&o_stbuf);
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]) (internal node: %s): OK (3600,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",attrstr);
		return fs::AttrReply{o_stbuf, 3600.0};
	}

	status = EINVAL;
	if ((to_set & (FUSE_SET_ATTR_MODE
			| FUSE_SET_ATTR_UID
			| FUSE_SET_ATTR_GID
			| FUSE_SET_ATTR_ATIME
			| FUSE_SET_ATTR_ATIME_NOW
			| FUSE_SET_ATTR_MTIME
			| FUSE_SET_ATTR_MTIME_NOW
			| FUSE_SET_ATTR_SIZE)) == 0) { // change other flags or change nothing
		status = fs_setattr(ino,ctx.uid,ctx.gid,0,0,0,0,0,0,0,attr);    // ext3 compatibility - change ctime during this operation (usually chown(-1,-1))
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),strerr(status));
			throw FsException(status);
		}
	}
	if (to_set & (FUSE_SET_ATTR_MODE
			| FUSE_SET_ATTR_UID
			| FUSE_SET_ATTR_GID
			| FUSE_SET_ATTR_ATIME
			| FUSE_SET_ATTR_MTIME
			| FUSE_SET_ATTR_ATIME_NOW
			| FUSE_SET_ATTR_MTIME_NOW)) {
		uint8_t setmask = 0;
		if (to_set & FUSE_SET_ATTR_MODE) {
			setmask |= SET_MODE_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_UID) {
			setmask |= SET_UID_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_GID) {
			setmask |= SET_GID_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_ATIME) {
			setmask |= SET_ATIME_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
			setmask |= SET_ATIME_NOW_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_MTIME) {
			setmask |= SET_MTIME_FLAG;
		}
		if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
			setmask |= SET_MTIME_NOW_FLAG;
		}
		if (to_set & (FUSE_SET_ATTR_MTIME | FUSE_SET_ATTR_MTIME_NOW)) {
			// in this case we want flush all pending writes because they could overwrite mtime
			write_data_flush_inode(ino);
		}
		status = fs_setattr(ino,ctx.uid,ctx.gid,setmask,stbuf->st_mode&07777,stbuf->st_uid,stbuf->st_gid,stbuf->st_atime,stbuf->st_mtime,sugid_clear_mode,attr);
		status = mfs_errorconv(status);
		if (status!=0) {
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),strerr(status));
			throw FsException(status);
		}
	}
	if (to_set & FUSE_SET_ATTR_SIZE) {
		if (stbuf->st_size<0) {
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),strerr(EINVAL));
			throw FsException(EINVAL);
		}
		if (stbuf->st_size>=MAX_FILE_SIZE) {
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),strerr(EFBIG));
			throw FsException(EFBIG);
		}
		write_data_flush_inode(ino);
		status = fs_truncate(ino,(fi!=NULL)?1:0,ctx.uid,ctx.gid,stbuf->st_size,attr);
		status = mfs_errorconv(status);
		read_inode_ops(ino);
		if (status!=0) {
			oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),strerr(status));
			throw FsException(status);
		}
	}
	if (status!=0) {        // should never happend but better check than sorry
		oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): %s",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),strerr(status));
		throw FsException(status);
	}
	if (attr[0]==TYPE_FILE) {
		maxfleng = write_data_getmaxfleng(ino);
	} else {
		maxfleng = 0;
	}
	memset(&o_stbuf, 0, sizeof(struct stat));
	mfs_attr_to_stat(ino,attr,&o_stbuf);
	if (maxfleng>(uint64_t)(o_stbuf.st_size)) {
		o_stbuf.st_size=maxfleng;
	}
	attr_timeout = (mfs_attr_get_mattr(attr)&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	mfs_makeattrstr(attrstr,256,&o_stbuf);
	oplog_printf(ctx,"setattr (%lu,0x%X,[%s:0%04o,%ld,%ld,%lu,%lu,%" PRIu64 "]): OK (%.1f,%s)",(unsigned long int)ino,to_set,modestr+1,(unsigned int)(stbuf->st_mode & 07777),(long int)stbuf->st_uid,(long int)stbuf->st_gid,(unsigned long int)(stbuf->st_atime),(unsigned long int)(stbuf->st_mtime),(uint64_t)(stbuf->st_size),attr_timeout,attrstr);
	return fs::AttrReply{o_stbuf, attr_timeout};
}

FsEntryParam fs::mknod(fs_ctx ctx, inode_t parent, const char *name, mode_t mode, dev_t rdev) {
	FsEntryParam e;
	uint32_t inode;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;
	uint8_t type;

	mfs_makemodestr(modestr,mode);
	mfs_stats_inc(OP_MKNOD);
	if (debug_mode) {
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX) ...",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev);
		fprintf(stderr,"mknod (%lu,%s,%s:0%04o,0x%08lX)\n",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev);
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}
	if (S_ISFIFO(mode)) {
		type = TYPE_FIFO;
	} else if (S_ISCHR(mode)) {
		type = TYPE_CHARDEV;
	} else if (S_ISBLK(mode)) {
		type = TYPE_BLOCKDEV;
	} else if (S_ISSOCK(mode)) {
		type = TYPE_SOCKET;
	} else if (S_ISREG(mode) || (mode&0170000)==0) {
		type = TYPE_FILE;
	} else {
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(EPERM));
		throw FsException(EPERM);
	}

	if (parent==MFS_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(EACCES));
			throw FsException(EACCES);
		}
	}

	status = fs_mknod(parent,nleng,(const uint8_t*)name,type,mode&07777,ctx.umask,ctx.uid,ctx.gid,rdev,inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): %s",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,strerr(status));
		throw FsException(status);
	} else {
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx,"mknod (%lu,%s,%s:0%04o,0x%08lX): OK (%.1f,%lu,%.1f,%s)",(unsigned long int)parent,name,modestr,(unsigned int)mode,(unsigned long int)rdev,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
		return e;
	}
}

void fs::unlink(fs_ctx ctx, inode_t parent, const char *name) {
	uint32_t nleng;
	int status;

	mfs_stats_inc(OP_UNLINK);
	if (debug_mode) {
		oplog_printf(ctx,"unlink (%lu,%s) ...",(unsigned long int)parent,name);
		fprintf(stderr,"unlink (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==MFS_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx,"unlink (%lu,%s): %s",(unsigned long int)parent,name,strerr(EACCES));
			throw FsException(EACCES);
		}
	}

	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx,"unlink (%lu,%s): %s",(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}

	status = fs_unlink(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"unlink (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
		throw FsException(status);
	} else {
		oplog_printf(ctx,"unlink (%lu,%s): OK",(unsigned long int)parent,name);
		return;
	}
}

// TODO search for 'throw FsException(0)'

// TODO search for stupid memsets
// memset(&e, 0, sizeof(e));

FsEntryParam fs::mkdir(fs_ctx ctx, inode_t parent, const char *name, mode_t mode) {
	struct FsEntryParam e;
	uint32_t inode;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;

	mfs_makemodestr(modestr,mode);
	mfs_stats_inc(OP_MKDIR);
	if (debug_mode) {
		oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o) ...",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
		fprintf(stderr,"mkdir (%lu,%s,d%s:0%04o)\n",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
	}
	if (parent==MFS_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(EACCES));
			throw FsException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}

	status = fs_mkdir(parent,nleng,(const uint8_t*)name,mode,ctx.umask,ctx.uid,ctx.gid,mkdir_copy_sgid,inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
		throw FsException(status);
	} else {
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:direntry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx,"mkdir (%lu,%s,d%s:0%04o): OK (%.1f,%lu,%.1f,%s)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
		return e;
	}
}

void fs::rmdir(fs_ctx ctx, inode_t parent, const char *name) {
	uint32_t nleng;
	int status;

	mfs_stats_inc(OP_RMDIR);
	if (debug_mode) {
		oplog_printf(ctx,"rmdir (%lu,%s) ...",(unsigned long int)parent,name);
		fprintf(stderr,"rmdir (%lu,%s)\n",(unsigned long int)parent,name);
	}
	if (parent==MFS_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(EACCES));
			throw FsException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}

	status = fs_rmdir(parent,nleng,(const uint8_t*)name,ctx.uid,ctx.gid);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"rmdir (%lu,%s): %s",(unsigned long int)parent,name,strerr(status));
		throw FsException(status);
	} else {
		oplog_printf(ctx,"rmdir (%lu,%s): OK",(unsigned long int)parent,name);
		return;
	}
}

FsEntryParam fs::symlink(fs_ctx ctx, const char *path, inode_t parent,
			 const char *name) {
	struct FsEntryParam e;
	uint32_t inode;
	uint8_t attr[35];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;

	mfs_stats_inc(OP_SYMLINK);
	if (debug_mode) {
		oplog_printf(ctx,"symlink (%s,%lu,%s) ...",path,(unsigned long int)parent,name);
		fprintf(stderr,"symlink (%s,%lu,%s)\n",path,(unsigned long int)parent,name);
	}
	if (parent==MFS_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx,"symlink (%s,%lu,%s): %s",path,(unsigned long int)parent,name,strerr(EACCES));
			throw FsException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx,"symlink (%s,%lu,%s): %s",path,(unsigned long int)parent,name,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}

	status = fs_symlink(parent,nleng,(const uint8_t*)name,(const uint8_t*)path,ctx.uid,ctx.gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"symlink (%s,%lu,%s): %s",path,(unsigned long int)parent,name,strerr(status));
		throw FsException(status);
	} else {
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx,"symlink (%s,%lu,%s): OK (%.1f,%lu,%.1f,%s)",path,(unsigned long int)parent,name,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
		return e;
	}
}

std::string fs::readlink(fs_ctx ctx, inode_t ino) {
	int status;
	const uint8_t *path;

	if (debug_mode) {
		oplog_printf(ctx,"readlink (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"readlink (%lu)\n",(unsigned long int)ino);
	}
	if (symlink_cache_search(ino,&path)) {
		mfs_stats_inc(OP_READLINK_CACHED);
		oplog_printf(ctx,"readlink (%lu) (using cache): OK (%s)",(unsigned long int)ino,(char*)path);
		return std::string((char*)path);
	}
	mfs_stats_inc(OP_READLINK);
	status = fs_readlink(ino,&path);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"readlink (%lu): %s",(unsigned long int)ino,strerr(status));
		throw FsException(status);
	} else {
		symlink_cache_insert(ino,path);
		oplog_printf(ctx,"readlink (%lu): OK (%s)",(unsigned long int)ino,(char*)path);
		return std::string((char*)path);
	}
}

void fs::rename(fs_ctx ctx, inode_t parent, const char *name,
			inode_t newparent, const char *newname) {
	uint32_t nleng,newnleng;
	int status;
	uint32_t inode;
	uint8_t attr[35];

	mfs_stats_inc(OP_RENAME);
	if (debug_mode) {
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s) ...",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
		fprintf(stderr,"rename (%lu,%s,%lu,%s)\n",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
	}
	if (parent==MFS_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(EACCES));
			throw FsException(EACCES);
		}
	}
	if (newparent==MFS_ROOT_ID) {
		if (IS_SPECIAL_NAME(newname)) {
			oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(EACCES));
			throw FsException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}

	status = fs_rename(parent,nleng,(const uint8_t*)name,newparent,newnleng,(const uint8_t*)newname,ctx.uid,ctx.gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s): %s",(unsigned long int)parent,name,(unsigned long int)newparent,newname,strerr(status));
		throw FsException(status);
	} else {
		oplog_printf(ctx,"rename (%lu,%s,%lu,%s): OK",(unsigned long int)parent,name,(unsigned long int)newparent,newname);
		return;
	}
}

FsEntryParam fs::link(fs_ctx ctx, inode_t ino, inode_t newparent, const char *newname) {
	uint32_t newnleng;
	int status;
	FsEntryParam e;
	uint32_t inode;
	uint8_t attr[35];
	char attrstr[256];
	uint8_t mattr;


	mfs_stats_inc(OP_LINK);
	if (debug_mode) {
		oplog_printf(ctx,"link (%lu,%lu,%s) ...",(unsigned long int)ino,(unsigned long int)newparent,newname);
		fprintf(stderr,"link (%lu,%lu,%s)\n",(unsigned long int)ino,(unsigned long int)newparent,newname);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(EACCES));
		throw FsException(EACCES);
	}
	if (newparent==MFS_ROOT_ID) {
		if (IS_SPECIAL_NAME(newname)) {
			oplog_printf(ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(EACCES));
			throw FsException(EACCES);
		}
	}
	newnleng = strlen(newname);
	if (newnleng>MFS_NAME_MAX) {
		oplog_printf(ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}

	status = fs_link(ino,newparent,newnleng,(const uint8_t*)newname,ctx.uid,ctx.gid,&inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"link (%lu,%lu,%s): %s",(unsigned long int)ino,(unsigned long int)newparent,newname,strerr(status));
		throw FsException(status);
	} else {
		memset(&e, 0, sizeof(e));
		e.ino = inode;
		mattr = mfs_attr_get_mattr(attr);
		e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
		e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
		mfs_attr_to_stat(inode,attr,&e.attr);
		mfs_makeattrstr(attrstr,256,&e.attr);
		oplog_printf(ctx,"link (%lu,%lu,%s): OK (%.1f,%lu,%.1f,%s)",(unsigned long int)ino,(unsigned long int)newparent,newname,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr);
		return e;
	}
}

void fs::opendir(fs_ctx ctx, inode_t ino, fs_file_info* fi) {
	dirbuf *dirinfo;
	int status;

	mfs_stats_inc(OP_OPENDIR);
	if (debug_mode) {
		oplog_printf(ctx,"opendir (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"opendir (%lu)\n",(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx,"opendir (%lu): %s",(unsigned long int)ino,strerr(ENOTDIR));
		throw FsException(ENOTDIR);
	}
	status = fs_access(ino,ctx.uid,ctx.gid,MODE_MASK_R);    // at least test rights
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"opendir (%lu): %s",(unsigned long int)ino,strerr(status));
		throw FsException(status);
	} else {
		dirinfo = (dirbuf*) malloc(sizeof(dirbuf));
		pthread_mutex_init(&(dirinfo->lock),NULL);
		pthread_mutex_lock(&(dirinfo->lock));   // make valgrind happy
		dirinfo->p = NULL;
		dirinfo->size = 0;
		dirinfo->dcache = NULL;
		dirinfo->wasread = 0;
		pthread_mutex_unlock(&(dirinfo->lock)); // make valgrind happy
		fi->fh = (unsigned long)dirinfo;
// TODO tutaj skonczylem i nie wiem co dalej, chyba przeniesc do funkcji wrappujacej
//		if (fuse_reply_open(req,fi) == -ENOENT) {
//			fi->fh = 0;
//			pthread_mutex_destroy(&(dirinfo->lock));
//			free(dirinfo);
//		}
		oplog_printf(ctx,"opendir (%lu): OK",(unsigned long int)ino);
	}
}

std::vector<fs::FsDirEntry> fs::readdir(fs_ctx ctx, inode_t ino, size_t size, off_t off,
			fs_file_info* fi) {
	int status;
	dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));
	char buffer[READDIR_BUFFSIZE];
	char name[MFS_NAME_MAX+1];
	const uint8_t *ptr,*eptr;
	uint8_t end;
	uint8_t nleng;
	uint32_t inode;
	uint8_t type;
	struct stat stbuf;

	mfs_stats_inc(OP_READDIR);
	if (debug_mode) {
		oplog_printf(ctx,"readdir (%lu,%" PRIu64 ",%" PRIu64 ") ...",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
		fprintf(stderr,"readdir (%lu,%" PRIu64 ",%" PRIu64 ")\n",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
	}
	if (off<0) {
		oplog_printf(ctx,"readdir (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(EINVAL));
		throw FsException(EINVAL);
	}
	pthread_mutex_lock(&(dirinfo->lock));
	if (dirinfo->wasread==0 || (dirinfo->wasread==1 && off==0)) {
		const uint8_t *dbuff;
		uint32_t dsize;
		uint8_t needscopy;
		if (usedircache) {
			status = fs_getdir_plus(ino,ctx.uid,ctx.gid,0,&dbuff,&dsize);
			if (status==0) {
				mfs_stats_inc(OP_GETDIR_FULL);
			}
			needscopy = 1;
			dirinfo->dataformat = 1;
		} else {
			status = fs_getdir(ino,ctx.uid,ctx.gid,&dbuff,&dsize);
			if (status==0) {
				mfs_stats_inc(OP_GETDIR_SMALL);
			}
			needscopy = 1;
			dirinfo->dataformat = 0;
		}
		status = mfs_errorconv(status);
		if (status!=0) {
			pthread_mutex_unlock(&(dirinfo->lock));
			oplog_printf(ctx,"readdir (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(status));
			throw FsException(status);
		}
		if (dirinfo->dcache) {
			dcache_release(dirinfo->dcache);
			dirinfo->dcache = NULL;
		}
		if (dirinfo->p) {
			free((uint8_t*)(dirinfo->p));
			dirinfo->p = NULL;
		}
		if (needscopy) {
			dirinfo->p = (const uint8_t*) malloc(dsize);
			if (dirinfo->p == NULL) {
				pthread_mutex_unlock(&(dirinfo->lock));
				oplog_printf(ctx,"readdir (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(EINVAL));
				throw FsException(EINVAL);
			}
			memcpy((uint8_t*)(dirinfo->p),dbuff,dsize);
		} else {
			dirinfo->p = dbuff;
		}
		dirinfo->size = dsize;
		if (usedircache && dirinfo->dataformat==1) {
			dirinfo->dcache = dcache_new(&ctx,ino,dirinfo->p,dirinfo->size);
		}
	}
	dirinfo->wasread=1;

	std::vector<fs::FsDirEntry> ret;
	if (off>=(off_t)(dirinfo->size)) {;
		oplog_printf(ctx,"readdir (%lu,%" PRIu64 ",%" PRIu64 "): OK (no data)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
	} else {
		if (size>READDIR_BUFFSIZE) {
			size=READDIR_BUFFSIZE;
		}
		ptr = dirinfo->p+off;
		eptr = dirinfo->p+dirinfo->size;
		end = 0;

		while (ptr<eptr && end==0) {
			nleng = ptr[0];
			ptr++;
			memcpy(name,ptr,nleng);
			name[nleng]=0;
			ptr+=nleng;
			off+=nleng+((dirinfo->dataformat)?40:6);
			if (ptr+5<=eptr) {
				inode = get32bit(&ptr);
				if (dirinfo->dataformat) {
					mfs_attr_to_stat(inode,ptr,&stbuf);
					ptr+=35;
				} else {
					type = get8bit(&ptr);
					mfs_type_to_stat(inode,type,&stbuf);
				}
				ret.push_back(FsDirEntry{name, stbuf, off, size});
			}
		}

		oplog_printf(ctx,"readdir (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,(unsigned long int)ret.size());
	}
	pthread_mutex_unlock(&(dirinfo->lock));
	return ret;
}

void fs::releasedir(fs_ctx ctx, inode_t ino, fs_file_info* fi) {
	(void)ino;
	dirbuf *dirinfo = (dirbuf *)((unsigned long)(fi->fh));

	mfs_stats_inc(OP_RELEASEDIR);
	if (debug_mode) {
		oplog_printf(ctx,"releasedir (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"releasedir (%lu)\n",(unsigned long int)ino);
	}
	pthread_mutex_lock(&(dirinfo->lock));
	pthread_mutex_unlock(&(dirinfo->lock));
	pthread_mutex_destroy(&(dirinfo->lock));
	if (dirinfo->dcache) {
		dcache_release(dirinfo->dcache);
	}
	if (dirinfo->p) {
		free((uint8_t*)(dirinfo->p));
	}
	free(dirinfo);
	fi->fh = 0;
	oplog_printf(ctx,"releasedir (%lu): OK",(unsigned long int)ino);
}


static finfo* fs_newfileinfo(uint8_t accmode,uint32_t inode) {
	finfo *fileinfo;
	fileinfo = (finfo*) malloc(sizeof(finfo));
	pthread_mutex_init(&(fileinfo->lock),NULL);
	pthread_mutex_lock(&(fileinfo->lock)); // make helgrind happy
#ifdef __FreeBSD__
	/* old FreeBSD fuse reads whole file when opening with O_WRONLY|O_APPEND,
	 * so can't open it write-only */
	(void)accmode;
	(void)inode;
	fileinfo->mode = IO_NONE;
	fileinfo->data = NULL;
#else
	if (accmode == O_RDONLY) {
		fileinfo->mode = IO_READONLY;
		fileinfo->data = read_data_new(inode);
	} else if (accmode == O_WRONLY) {
		fileinfo->mode = IO_WRITEONLY;
		fileinfo->data = write_data_new(inode);
	} else {
		fileinfo->mode = IO_NONE;
		fileinfo->data = NULL;
	}
#endif
	pthread_mutex_unlock(&(fileinfo->lock)); // make helgrind happy
	return fileinfo;
}

// TODO przejrzeć wszystkie użycia
void fs::remove_file_info(fs_file_info *f) {
	finfo* fileinfo = (finfo*)(f->fh);
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode == IO_READONLY || fileinfo->mode == IO_READ) {
		read_data_end(fileinfo->data);
	} else if (fileinfo->mode == IO_WRITEONLY || fileinfo->mode == IO_WRITE) {
		write_data_end(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	pthread_mutex_destroy(&(fileinfo->lock));
	free(fileinfo);
}

void fs::remove_dir_info(fs_file_info *fi) {
	dirbuf* dirinfo = (dirbuf*) fi->fh;
	fi->fh = 0;
	pthread_mutex_destroy(&(dirinfo->lock));
	free(dirinfo);
}



FsEntryParam fs::create(fs_ctx ctx, inode_t parent, const char *name, mode_t mode,
		fs_file_info* fi) {
	struct FsEntryParam e;
	uint32_t inode;
	uint8_t oflags;
	uint8_t attr[35];
	char modestr[11];
	char attrstr[256];
	uint8_t mattr;
	uint32_t nleng;
	int status;

	finfo *fileinfo;

	mfs_makemodestr(modestr,mode);
	mfs_stats_inc(OP_CREATE);
	if (debug_mode) {
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
		fprintf(stderr,"create (%lu,%s,-%s:0%04o)\n",(unsigned long int)parent,name,modestr+1,(unsigned int)mode);
	}
	if (parent==MFS_ROOT_ID) {
		if (IS_SPECIAL_NAME(name)) {
			oplog_printf(ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(EACCES));
			throw FsException(EACCES);
		}
	}
	nleng = strlen(name);
	if (nleng>MFS_NAME_MAX) {
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(ENAMETOOLONG));
		throw FsException(ENAMETOOLONG);
	}

	oflags = AFTER_CREATE;
	if ((fi->flags & O_ACCMODE) == O_RDONLY) {
		oflags |= WANT_READ;
	} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
		oflags |= WANT_WRITE;
	} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
		oflags |= WANT_READ | WANT_WRITE;
	} else {
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(EINVAL));
		throw FsException(EINVAL);
	}

	status = fs_mknod(parent,nleng,(const uint8_t*)name,TYPE_FILE,mode&07777,ctx.umask,ctx.uid,ctx.gid,0,inode,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o) (mknod): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
		throw FsException(status);
	}
	status = fs_opencheck(inode,ctx.uid,ctx.gid,oflags,NULL);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"create (%lu,%s,-%s:0%04o) (open): %s",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,strerr(status));
		throw FsException(status);
	}

	mattr = mfs_attr_get_mattr(attr);
	fileinfo = fs_newfileinfo(fi->flags & O_ACCMODE,inode);
	fi->fh = (unsigned long)fileinfo;
	if (keep_cache==1) {
		fi->keep_cache=1;
	} else if (keep_cache==2) {
		fi->keep_cache=0;
	} else {
		fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"create (%lu) ok -> keep cache: %lu\n",(unsigned long int)inode,(unsigned long int)fi->keep_cache);
	}
	e.ino = inode;
	e.attr_timeout = (mattr&MATTR_NOACACHE)?0.0:attr_cache_timeout;
	e.entry_timeout = (mattr&MATTR_NOECACHE)?0.0:entry_cache_timeout;
	mfs_attr_to_stat(inode,attr,&e.attr);
// TODO use following code in wrapping function
//	if (fuse_reply_create(req, &e, fi) == -ENOENT) {
//		fs::removefileinfo(fileinfo);
//	}
	mfs_makeattrstr(attrstr,256,&e.attr);
	oplog_printf(ctx,"create (%lu,%s,-%s:0%04o): OK (%.1f,%lu,%.1f,%s,%lu)",(unsigned long int)parent,name,modestr+1,(unsigned int)mode,e.entry_timeout,(unsigned long int)e.ino,e.attr_timeout,attrstr,(unsigned long int)fi->keep_cache);
	return e;
}

void fs::open(fs_ctx ctx, inode_t ino, fs_file_info* fi) {
	uint8_t oflags;
	uint8_t attr[35];
	uint8_t mattr;
	int status;

	finfo *fileinfo;

	mfs_stats_inc(OP_OPEN);
	if (debug_mode) {
		oplog_printf(ctx,"open (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"open (%lu)\n",(unsigned long int)ino);
	}

	if (ino==MASTERINFO_INODE) {
		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
			oplog_printf(ctx,"open (%lu) (internal node: MASTERINFO): %s",(unsigned long int)ino,strerr(EACCES));
			throw FsException(EACCES);
		}
		fi->fh = 0;
		fi->direct_io = 0;
		fi->keep_cache = 1;
		oplog_printf(ctx,"open (%lu) (internal node: MASTERINFO): OK (0,1)",(unsigned long int)ino);
		return;
	}

	if (ino==STATS_INODE) {
		sinfo *statsinfo;
		statsinfo = (sinfo*) malloc(sizeof(sinfo));
		if (statsinfo==NULL) {
			oplog_printf(ctx,"open (%lu) (internal node: STATS): %s",(unsigned long int)ino,strerr(ENOMEM));
			throw FsException(ENOMEM);
		}
		pthread_mutex_init(&(statsinfo->lock),NULL);    // make helgrind happy
		pthread_mutex_lock(&(statsinfo->lock));         // make helgrind happy
		stats_show_all(&(statsinfo->buff),&(statsinfo->leng));
		statsinfo->reset = 0;
		pthread_mutex_unlock(&(statsinfo->lock));       // make helgrind happy
		fi->fh = (unsigned long)statsinfo;
		fi->direct_io = 1;
		fi->keep_cache = 0;
		oplog_printf(ctx,"open (%lu) (internal node: STATS): OK (1,0)",(unsigned long int)ino);
		return;
	}

	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		if ((fi->flags & O_ACCMODE) != O_RDONLY) {
			oplog_printf(ctx,"open (%lu) (internal node: %s): %s",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY",strerr(EACCES));
			throw FsException(EACCES);
		}
		fi->fh = oplog_newhandle((ino==OPHISTORY_INODE)?1:0);
		fi->direct_io = 1;
		fi->keep_cache = 0;
		oplog_printf(ctx,"open (%lu) (internal node: %s): OK (1,0)",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY");
		return;
	}

	oflags = 0;
	if ((fi->flags & O_ACCMODE) == O_RDONLY) {
		oflags |= WANT_READ;
	} else if ((fi->flags & O_ACCMODE) == O_WRONLY) {
		oflags |= WANT_WRITE;
	} else if ((fi->flags & O_ACCMODE) == O_RDWR) {
		oflags |= WANT_READ | WANT_WRITE;
	}
	status = fs_opencheck(ino,ctx.uid,ctx.gid,oflags,attr);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"open (%lu): %s",(unsigned long int)ino,strerr(status));
		throw FsException(status);
	}

	mattr = mfs_attr_get_mattr(attr);
	fileinfo = fs_newfileinfo(fi->flags & O_ACCMODE,ino);
	fi->fh = (unsigned long)fileinfo;
	if (keep_cache==1) {
		fi->keep_cache=1;
	} else if (keep_cache==2) {
		fi->keep_cache=0;
	} else {
		fi->keep_cache = (mattr&MATTR_ALLOWDATACACHE)?1:0;
	}
	if (debug_mode) {
		fprintf(stderr,"open (%lu) ok -> keep cache: %lu\n",(unsigned long int)ino,(unsigned long int)fi->keep_cache);
	}
	fi->direct_io = 0;
// TODO move to fuse wrapper
//	if (fuse_reply_open(req, fi) == -ENOENT) {
//		fs::removefileinfo(fileinfo);
//	}
	oplog_printf(ctx,"open (%lu): OK (%lu,%lu)",(unsigned long int)ino,(unsigned long int)fi->direct_io,(unsigned long int)fi->keep_cache);
}

void fs::release(fs_ctx ctx, inode_t ino, fs_file_info* fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);

	mfs_stats_inc(OP_RELEASE);
	if (debug_mode) {
		oplog_printf(ctx,"release (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"release (%lu)\n",(unsigned long int)ino);
	}

	if (ino==MASTERINFO_INODE) {
		oplog_printf(ctx,"release (%lu) (internal node: MASTERINFO): OK",(unsigned long int)ino);
		return;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));         // make helgrind happy
			if (statsinfo->buff!=NULL) {
				free(statsinfo->buff);
			}
			if (statsinfo->reset) {
				stats_reset_all();
			}
			pthread_mutex_unlock(&(statsinfo->lock));       // make helgrind happy
			pthread_mutex_destroy(&(statsinfo->lock));      // make helgrind happy
			free(statsinfo);
		}
		oplog_printf(ctx,"release (%lu) (internal node: STATS): OK",(unsigned long int)ino);
		return;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		oplog_releasehandle(fi->fh);
		oplog_printf(ctx,"release (%lu) (internal node: %s): OK",(unsigned long int)ino,(ino==OPLOG_INODE)?"OPLOG":"OPHISTORY");
		return;
	}
	if (fileinfo!=NULL) {
		fs::remove_file_info(fi);
	}
	fs_release(ino);
	oplog_printf(ctx,"release (%lu): OK",(unsigned long int)ino);
}

std::vector<uint8_t> fs::read(fs_ctx ctx, inode_t ino, size_t size, off_t off,
			fs_file_info* fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	uint8_t *buff;
	uint32_t ssize;
	int err;
	std::vector<uint8_t> ret;

	mfs_stats_inc(OP_READ);
	if (debug_mode) {
		if (ino!=OPLOG_INODE && ino!=OPHISTORY_INODE) {
			oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 ") ...",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
		}
		fprintf(stderr,"read from inode %lu up to %" PRIu64 " bytes from position %" PRIu64 "\n",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
	}
	if (ino==MASTERINFO_INODE) {
		uint8_t masterinfo[14];
		fs_getmasterlocation(masterinfo);
		masterproxy_getlocation(masterinfo);
		if (off>=14) {
			oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): OK (no data)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
		} else if (off+size>14) {
			std::copy(masterinfo + off, masterinfo + 14, std::back_inserter(ret));
			oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,(unsigned long int)(14-off));
		} else {
			std::copy(masterinfo + off, masterinfo + off + size, std::back_inserter(ret));
			oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,(unsigned long int)size);
		}
		return ret;
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));         // make helgrind happy
			if (off>=statsinfo->leng) {
				oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): OK (no data)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
			} else if ((uint64_t)(off+size)>(uint64_t)(statsinfo->leng)) {
				std::copy(statsinfo->buff + off, statsinfo->buff + statsinfo->leng, std::back_inserter(ret));
				oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,(unsigned long int)(statsinfo->leng-off));
			} else {
				std::copy(statsinfo->buff + off, statsinfo->buff + off + size, std::back_inserter(ret));
				oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,(unsigned long int)size);
			}
			pthread_mutex_unlock(&(statsinfo->lock));       // make helgrind happy
		} else {
			oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): OK (no data)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
		}
		return ret;
	}
	if (ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		oplog_getdata(fi->fh,&buff,&ssize,size);
		oplog_releasedata(fi->fh);
		return std::vector<uint8_t>(buff, buff + ssize);
	}
	if (fileinfo==NULL) {
		oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(EBADF));
		throw FsException(EBADF);
	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(EFBIG));
		throw FsException(EFBIG);
	}
	try {
		const SteadyTimePoint deadline = SteadyClock::now() + std::chrono::seconds(30);
		uint8_t status = gLocalIoLimiter().waitForRead(ctx.pid, size, deadline);
		if (status == STATUS_OK) {
			status = gGlobalIoLimiter().waitForRead(ctx.pid, size, deadline);
		}
		if (status != STATUS_OK) {
			throw FsException(status);
		}
	} catch (Exception& ex) {
		syslog(LOG_WARNING, "I/O limiting error: %s", ex.what());
		throw FsException(EIO);
	}
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITEONLY) {
		pthread_mutex_unlock(&(fileinfo->lock));
		oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(EACCES));
		throw FsException(EACCES);
	}
	if (fileinfo->mode==IO_WRITE) {
		err = write_data_flush(fileinfo->data);
		if (err!=0) {
			pthread_mutex_unlock(&(fileinfo->lock));
			if (debug_mode) {
				fprintf(stderr,"IO error occured while writting inode %lu\n",(unsigned long int)ino);
			}
			oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(err));
			throw FsException(err);
		}
		write_data_end(fileinfo->data);
	}
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_NONE) {
		fileinfo->mode = IO_READ;
		fileinfo->data = read_data_new(ino);
	}
	write_data_flush_inode(ino);
	ssize = size;
	buff = NULL;    // use internal 'readdata' buffer
	err = read_data(fileinfo->data,off,&ssize,&buff);
	if (err!=0) {
// TODO sprawdzic czy rename fuse_reply_err => throw FsExsception nie narobił deadlocków
		if (debug_mode) {
			fprintf(stderr,"IO error occured while reading inode %lu\n",(unsigned long int)ino);
		}
		oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(err));
		read_data_freebuff(fileinfo->data);
		pthread_mutex_unlock(&(fileinfo->lock));
		throw FsException(err);
	} else {
		if (debug_mode) {
			fprintf(stderr,"%" PRIu32 " bytes have been read from inode %lu\n",ssize,(unsigned long int)ino);
		}
		oplog_printf(ctx,"read (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,(unsigned long int)ssize);
		ret = std::vector<uint8_t>(buff, buff + ssize);
	}
	read_data_freebuff(fileinfo->data);
	pthread_mutex_unlock(&(fileinfo->lock)); // TODO raii
	return ret;
}

fs::BytesWritten fs::write(fs_ctx ctx, inode_t ino, const char *buf, size_t size, off_t off,
			fs_file_info* fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;

	mfs_stats_inc(OP_WRITE);
	if (debug_mode) {
		oplog_printf(ctx,"write (%lu,%" PRIu64 ",%" PRIu64 ") ...",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
		fprintf(stderr,"write to inode %lu %" PRIu64 " bytes at position %" PRIu64 "\n",(unsigned long int)ino,(uint64_t)size,(uint64_t)off);
	}
	if (ino==MASTERINFO_INODE || ino==OPLOG_INODE || ino==OPHISTORY_INODE) {
		oplog_printf(ctx,"write (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(EACCES));
		throw FsException(EACCES);
	}
	if (ino==STATS_INODE) {
		sinfo *statsinfo = (sinfo*)(unsigned long)(fi->fh);
		if (statsinfo!=NULL) {
			pthread_mutex_lock(&(statsinfo->lock));         // make helgrind happy
			statsinfo->reset=1;
			pthread_mutex_unlock(&(statsinfo->lock));       // make helgrind happy
		}
		oplog_printf(ctx,"write (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,(unsigned long int)size);
		return size;
	}
	if (fileinfo==NULL) {
		oplog_printf(ctx,"write (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(EBADF));
		throw FsException(EBADF);
	}
	if (off>=MAX_FILE_SIZE || off+size>=MAX_FILE_SIZE) {
		oplog_printf(ctx,"write (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(EFBIG));
		throw FsException(EFBIG);
	}
	try {
		const SteadyTimePoint deadline = SteadyClock::now() + std::chrono::seconds(30);
		uint8_t status = gLocalIoLimiter().waitForRead(ctx.pid, size, deadline);
		if (status == STATUS_OK) {
			status = gGlobalIoLimiter().waitForRead(ctx.pid, size, deadline);
		}
		if (status != STATUS_OK) {
			throw FsException(status);
		}
	} catch (Exception& ex) {
		syslog(LOG_WARNING, "I/O limiting error: %s", ex.what());
		throw FsException(EIO);
	}
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_READONLY) {
		pthread_mutex_unlock(&(fileinfo->lock));
		oplog_printf(ctx,"write (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(EACCES));
		throw FsException(EACCES);
	}
	if (fileinfo->mode==IO_READ) {
		read_data_end(fileinfo->data);
	}
	if (fileinfo->mode==IO_READ || fileinfo->mode==IO_NONE) {
		fileinfo->mode = IO_WRITE;
		fileinfo->data = write_data_new(ino);
	}
	err = write_data(fileinfo->data,off,size,(const uint8_t*)buf);
	if (err!=0) {
		pthread_mutex_unlock(&(fileinfo->lock));
		if (debug_mode) {
			fprintf(stderr,"IO error occured while writting inode %lu\n",(unsigned long int)ino);
		}
		oplog_printf(ctx,"write (%lu,%" PRIu64 ",%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,strerr(err));
		throw FsException(err);
	} else {
		pthread_mutex_unlock(&(fileinfo->lock));
		if (debug_mode) {
			fprintf(stderr,"%" PRIu64 " bytes have been written to inode %lu\n",(uint64_t)size,(unsigned long int)ino);
		}
		oplog_printf(ctx,"write (%lu,%" PRIu64 ",%" PRIu64 "): OK (%lu)",(unsigned long int)ino,(uint64_t)size,(uint64_t)off,(unsigned long int)size);
		return size;
	}
}

void fs::flush(fs_ctx ctx, inode_t ino, fs_file_info* fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;

	mfs_stats_inc(OP_FLUSH);
	if (debug_mode) {
		oplog_printf(ctx,"flush (%lu) ...",(unsigned long int)ino);
		fprintf(stderr,"flush (%lu)\n",(unsigned long int)ino);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx,"flush (%lu): OK",(unsigned long int)ino);
		throw FsException(0);
	}
	if (fileinfo==NULL) {
		oplog_printf(ctx,"flush (%lu): %s",(unsigned long int)ino,strerr(EBADF));
		throw FsException(EBADF);
	}
//      syslog(LOG_NOTICE,"remove_locks inode:%lu owner:%" PRIu64 "",(unsigned long int)ino,(uint64_t)fi->lock_owner);
	err = 0;
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	if (err!=0) {
		oplog_printf(ctx,"flush (%lu): %s",(unsigned long int)ino,strerr(err));
		throw FsException(err);
	} else {
		oplog_printf(ctx,"flush (%lu): OK",(unsigned long int)ino);
	}
}

void fs::fsync(fs_ctx ctx, inode_t ino, int datasync, fs_file_info* fi) {
	finfo *fileinfo = (finfo*)(unsigned long)(fi->fh);
	int err;

	mfs_stats_inc(OP_FSYNC);
	if (debug_mode) {
		oplog_printf(ctx,"fsync (%lu,%d) ...",(unsigned long int)ino,datasync);
		fprintf(stderr,"fsync (%lu,%d)\n",(unsigned long int)ino,datasync);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx,"fsync (%lu,%d): OK",(unsigned long int)ino,datasync);
		return;
	}
	if (fileinfo==NULL) {
		oplog_printf(ctx,"fsync (%lu,%d): %s",(unsigned long int)ino,datasync,strerr(EBADF));
		throw FsException(EBADF);
	}
	err = 0;
	pthread_mutex_lock(&(fileinfo->lock));
	if (fileinfo->mode==IO_WRITE || fileinfo->mode==IO_WRITEONLY) {
		err = write_data_flush(fileinfo->data);
	}
	pthread_mutex_unlock(&(fileinfo->lock));
	if (err!=0) {
		oplog_printf(ctx,"fsync (%lu,%d): %s",(unsigned long int)ino,datasync,strerr(err));
		throw FsException(err);
	} else {
		oplog_printf(ctx,"fsync (%lu,%d): OK",(unsigned long int)ino,datasync);
	}
}

namespace {

class XattrHandler {
public:
	virtual ~XattrHandler() {}

	/*
	 * handler for request to set an extended attribute
	 * mode - one of XATTR_SMODE_*
	 * returns status
	 */
	virtual uint8_t setxattr(const fs_ctx& ctx, inode_t ino, const char *name,
			uint32_t nleng, const char *value, size_t size, int mode) = 0;

	/*
	 * handler for request to get an extended attribute
	 * mode - one of XATTR_GMODE_*
	 * returns status and:
	 * * sets value is mode is XATTR_GMODE_GET_DATA
	 * * sets valueLength is mode is XATTR_GMODE_LENGTH_ONLY
	 */
	virtual uint8_t getxattr(const fs_ctx& ctx, inode_t ino, const char *name,
			uint32_t nleng, int mode, uint32_t& valueLength, std::vector<uint8_t>& value) = 0;

	/*
	 * handler for request to remove an extended attribute
	 * returns status
	 */
	virtual uint8_t removexattr(const fs_ctx& ctx, inode_t ino, const char *name,
			uint32_t nleng) = 0;
};

class PlainXattrHandler : public XattrHandler {
public:
	virtual uint8_t setxattr(const fs_ctx& ctx, inode_t ino, const char *name,
			uint32_t nleng, const char *value, size_t size, int mode) {
		return fs_setxattr(ino, 0, ctx.uid, ctx.gid, nleng, (const uint8_t*)name,
				(uint32_t)size, (const uint8_t*)value, mode);
	}

	virtual uint8_t getxattr(const fs_ctx& ctx, inode_t ino, const char *name,
			uint32_t nleng, int mode, uint32_t& valueLength, std::vector<uint8_t>& value) {
		const uint8_t *buff;
		uint8_t status = fs_getxattr(ino, 0, ctx.uid, ctx.gid, nleng, (const uint8_t*)name,
				mode, &buff, &valueLength);
		if (mode == XATTR_GMODE_GET_DATA && status == STATUS_OK) {
			value = std::vector<uint8_t>(buff, buff + valueLength);
		}
		return status;
	}

	virtual uint8_t removexattr(const fs_ctx& ctx, inode_t ino, const char *name,
			uint32_t nleng) {
		return fs_removexattr(ino, 0, ctx.uid, ctx.gid, nleng, (const uint8_t*)name);
	}
};

class ErrorXattrHandler : public XattrHandler {
public:
	ErrorXattrHandler(uint8_t error) : error_(error) {}
	virtual uint8_t setxattr(const fs_ctx&, inode_t, const char *,
			uint32_t, const char *, size_t, int) {
		return error_;
	}

	virtual uint8_t getxattr(const fs_ctx&, inode_t, const char *,
			uint32_t, int, uint32_t&, std::vector<uint8_t>&) {
		return error_;
	}

	virtual uint8_t removexattr(const fs_ctx&, inode_t, const char *,
			uint32_t) {
		return error_;
	}
private:
	uint8_t error_;
};

class AclXattrHandler : public XattrHandler {
public:
	AclXattrHandler(AclType type) : type_(type) {}

	virtual uint8_t setxattr(const fs_ctx& ctx, inode_t ino, const char *,
			uint32_t, const char *value, size_t size, int) {
		if (!acl_enabled) {
			return ERROR_ENOTSUP;
		}
		AccessControlList acl;
		try {
			PosixAclXattr posix = aclConverter::extractPosixObject((const uint8_t*)value, size);
			if (posix.entries.empty()) {
				// Is empty ACL set? It means to remove it!
				return fs_deletacl(ino, ctx.uid, ctx.gid, type_);
			}
			acl = aclConverter::posixToAclObject(posix);
		} catch (Exception&) {
			return ERROR_EINVAL;
		}
		return fs_setacl(ino, ctx.uid, ctx.gid, type_, acl);
	}

	virtual uint8_t getxattr(const fs_ctx& ctx, inode_t ino, const char *,
			uint32_t, int /*mode*/, uint32_t& valueLength, std::vector<uint8_t>& value) {
		if (!acl_enabled) {
			return ERROR_ENOTSUP;
		}
		AccessControlList acl;
		uint8_t status = fs_getacl(ino, ctx.uid, ctx.gid, type_, acl);
		if (status != STATUS_OK) {
			return status;
		}
		try {
			value = aclConverter::aclObjectToXattr(acl);
			valueLength = value.size();
			return STATUS_OK;
		} catch (Exception&) {
			syslog(LOG_WARNING, "Failed to convert ACL to xattr, looks like a bug");
			return ERROR_IO;
		}
	}

	virtual uint8_t removexattr(const fs_ctx& ctx, inode_t ino, const char *,
			uint32_t) {
		if (!acl_enabled) {
			return ERROR_ENOTSUP;
		}
		return fs_deletacl(ino, ctx.uid, ctx.gid, type_);
	}

private:
	AclType type_;
};

} // anonymous namespace

static AclXattrHandler accessAclXattrHandler(AclType::kAccess);
static AclXattrHandler defaultAclXattrHandler(AclType::kDefault);
static ErrorXattrHandler enotsupXattrHandler(ERROR_ENOTSUP);
static PlainXattrHandler plainXattrHandler;

static std::map<std::string, XattrHandler*> xattr_handlers = {
	{POSIX_ACL_XATTR_ACCESS, &accessAclXattrHandler},
	{POSIX_ACL_XATTR_DEFAULT, &defaultAclXattrHandler},
	{"security.capability", &enotsupXattrHandler},
};

static XattrHandler* choose_xattr_handler(const char *name) {
	try {
		return xattr_handlers.at(name);
	} catch (std::out_of_range&) {
		return &plainXattrHandler;
	}
}

// TODO up
//#if defined(__APPLE__)
//void mfs_setxattr (fuse_req_t req, inode_t ino, const char *name, const char *value, size_t size, int flags, uint32_t position) {
//#else
//void mfs_setxattr (fuse_req_t req, inode_t ino, const char *name, const char *value, size_t size, int flags) {
//	uint32_t position=0;
//#endif
void fs::setxattr(fs_ctx ctx, inode_t ino, const char *name, const char *value,
			size_t size, int flags, uint32_t position) {
	uint32_t nleng;
	int status;
	uint8_t mode;


	mfs_stats_inc(OP_SETXATTR);
	if (debug_mode) {
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d) ...",(unsigned long int)ino,name,(uint64_t)size,flags);
		fprintf(stderr,"setxattr (%lu,%s,%" PRIu64 ",%d)",(unsigned long int)ino,name,(uint64_t)size,flags);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): %s",(unsigned long int)ino,name,(uint64_t)size,flags,strerr(EPERM));
		throw FsException(EPERM);
	}
	if (size>MFS_XATTR_SIZE_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns E2BIG here
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): %s",(unsigned long int)ino,name,(uint64_t)size,flags,strerr(E2BIG));
		throw FsException(E2BIG);
#else
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): %s",(unsigned long int)ino,name,(uint64_t)size,flags,strerr(ERANGE));
		throw FsException(ERANGE);
#endif
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): %s",(unsigned long int)ino,name,(uint64_t)size,flags,strerr(EPERM));
		throw FsException(EPERM);
#else
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): %s",(unsigned long int)ino,name,(uint64_t)size,flags,strerr(ERANGE));
		throw FsException(ERANGE);
#endif
	}
	if (nleng==0) {
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): %s",(unsigned long int)ino,name,(uint64_t)size,flags,strerr(EINVAL));
		throw FsException(EINVAL);
	}
	if (strcmp(name,"security.capability")==0) {
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): %s",(unsigned long int)ino,name,(uint64_t)size,flags,strerr(ENOTSUP));
		throw FsException(ENOTSUP);
	}
#if defined(XATTR_CREATE) && defined(XATTR_REPLACE)
	if ((flags&XATTR_CREATE) && (flags&XATTR_REPLACE)) {
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): %s",(unsigned long int)ino,name,(uint64_t)size,flags,strerr(EINVAL));
		throw FsException(EINVAL);
	}
	mode = (flags==XATTR_CREATE)?XATTR_SMODE_CREATE_ONLY:(flags==XATTR_REPLACE)?XATTR_SMODE_REPLACE_ONLY:XATTR_SMODE_CREATE_OR_REPLACE;
#else
	mode = 0;
#endif
	(void)position;
	status = choose_xattr_handler(name)->setxattr(ctx, ino, name, nleng, value, size, mode);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): %s",(unsigned long int)ino,name,(uint64_t)size,flags,strerr(status));
		throw FsException(status);
	}
	oplog_printf(ctx,"setxattr (%lu,%s,%" PRIu64 ",%d): OK",(unsigned long int)ino,name,(uint64_t)size,flags);
}

// TODO up
//#if defined(__APPLE__)
//void mfs_getxattr (fuse_req_t req, inode_t ino, const char *name, size_t size, uint32_t position) {
//#else
//void mfs_getxattr (fuse_req_t req, inode_t ino, const char *name, size_t size) {
//	uint32_t position=0;
//#endif /* __APPLE__ */
fs::XattrReply fs::getxattr(fs_ctx ctx, inode_t ino, const char *name, size_t size,
		uint32_t position) {
	uint32_t nleng;
	int status;
	uint8_t mode;
	std::vector<uint8_t> buffer;
	const uint8_t *buff;
	uint32_t leng;


	mfs_stats_inc(OP_GETXATTR);
	if (debug_mode) {
		oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 ") ...",(unsigned long int)ino,name,(uint64_t)size);
		fprintf(stderr,"getxattr (%lu,%s,%" PRIu64 ")",(unsigned long int)ino,name,(uint64_t)size);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 "): %s",(unsigned long int)ino,name,(uint64_t)size,strerr(ENODATA));
		throw FsException(ENODATA);
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 "): %s",(unsigned long int)ino,name,(uint64_t)size,strerr(EPERM));
		throw FsException(EPERM);
#else
		oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 "): %s",(unsigned long int)ino,name,(uint64_t)size,strerr(ERANGE));
		throw FsException(ERANGE);
#endif
	}
	if (nleng==0) {
		oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 "): %s",(unsigned long int)ino,name,(uint64_t)size,strerr(EINVAL));
		throw FsException(EINVAL);
	}
	if (strcmp(name,"security.capability")==0) {
		oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 "): %s",(unsigned long int)ino,name,(uint64_t)size,strerr(ENOTSUP));
		throw FsException(ENOTSUP);
	}
	if (size==0) {
		mode = XATTR_GMODE_LENGTH_ONLY;
	} else {
		mode = XATTR_GMODE_GET_DATA;
	}
	(void)position;
	status = choose_xattr_handler(name)->getxattr(ctx, ino, name, nleng, mode, leng, buffer);
	buff = buffer.data();
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 "): %s",(unsigned long int)ino,name,(uint64_t)size,strerr(status));
		throw FsException(status);
	}
	if (size==0) {
		oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 "): OK (%" PRIu32 ")",(unsigned long int)ino,name,(uint64_t)size,leng);
		return XattrReply{leng, {}};
	} else {
		if (leng>size) {
			oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 "): %s",(unsigned long int)ino,name,(uint64_t)size,strerr(ERANGE));
			throw FsException(ERANGE);
		} else {
			oplog_printf(ctx,"getxattr (%lu,%s,%" PRIu64 "): OK (%" PRIu32 ")",(unsigned long int)ino,name,(uint64_t)size,leng);
			return XattrReply{leng, std::vector<uint8_t>(buff, buff + leng)};
		}
	}
}

fs::XattrReply fs::listxattr(fs_ctx ctx, inode_t ino, size_t size) {
	const uint8_t *buff;
	uint32_t leng;
	int status;
	uint8_t mode;

	mfs_stats_inc(OP_LISTXATTR);
	if (debug_mode) {
		oplog_printf(ctx,"listxattr (%lu,%" PRIu64 ") ...",(unsigned long int)ino,(uint64_t)size);
		fprintf(stderr,"listxattr (%lu,%" PRIu64 ")",(unsigned long int)ino,(uint64_t)size);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx,"listxattr (%lu,%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,strerr(EPERM));
		throw FsException(EPERM);
	}
	if (size==0) {
		mode = XATTR_GMODE_LENGTH_ONLY;
	} else {
		mode = XATTR_GMODE_GET_DATA;
	}
	status = fs_listxattr(ino,0,ctx.uid,ctx.gid,mode,&buff,&leng);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"listxattr (%lu,%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,strerr(status));
		throw FsException(status);
	}
	if (size==0) {
		oplog_printf(ctx,"listxattr (%lu,%" PRIu64 "): OK (%" PRIu32 ")",(unsigned long int)ino,(uint64_t)size,leng);
		return XattrReply{leng, {}};
	} else {
		if (leng>size) {
			oplog_printf(ctx,"listxattr (%lu,%" PRIu64 "): %s",(unsigned long int)ino,(uint64_t)size,strerr(ERANGE));
			throw FsException(ERANGE);
		} else {
			oplog_printf(ctx,"listxattr (%lu,%" PRIu64 "): OK (%" PRIu32 ")",(unsigned long int)ino,(uint64_t)size,leng);
			return XattrReply{leng, std::vector<uint8_t>(buff, buff + leng)};
		}
	}
}

void fs::removexattr(fs_ctx ctx, inode_t ino, const char *name) {
	uint32_t nleng;
	int status;

	mfs_stats_inc(OP_REMOVEXATTR);
	if (debug_mode) {
		oplog_printf(ctx,"removexattr (%lu,%s) ...",(unsigned long int)ino,name);
		fprintf(stderr,"removexattr (%lu,%s)",(unsigned long int)ino,name);
	}
	if (IS_SPECIAL_INODE(ino)) {
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(EPERM));
		throw FsException(EPERM);
	}
	nleng = strlen(name);
	if (nleng>MFS_XATTR_NAME_MAX) {
#if defined(__APPLE__)
		// Mac OS X returns EPERM here
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(EPERM));
		throw FsException(EPERM);
#else
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(ERANGE));
		throw FsException(ERANGE);
#endif
	}
	if (nleng==0) {
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(EINVAL));
		throw FsException(EINVAL);
	}
	status = choose_xattr_handler(name)->removexattr(ctx, ino, name, nleng);
	status = mfs_errorconv(status);
	if (status!=0) {
		oplog_printf(ctx,"removexattr (%lu,%s): %s",(unsigned long int)ino,name,strerr(status));
		throw FsException(status);
	} else {
		oplog_printf(ctx,"removexattr (%lu,%s): OK",(unsigned long int)ino,name);
	}
}

void fs::init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_,
		double entry_cache_timeout_, double attr_cache_timeout_, int mkdir_copy_sgid_,
		int sugid_clear_mode_, bool acl_enabled_) {
	const char* sugid_clear_mode_strings[] = {SUGID_CLEAR_MODE_STRINGS};
	debug_mode = debug_mode_;
	keep_cache = keep_cache_;
	direntry_cache_timeout = direntry_cache_timeout_;
	entry_cache_timeout = entry_cache_timeout_;
	attr_cache_timeout = attr_cache_timeout_;
	mkdir_copy_sgid = mkdir_copy_sgid_;
	sugid_clear_mode = sugid_clear_mode_;
	acl_enabled = acl_enabled_;
	if (debug_mode) {
		fprintf(stderr,"cache parameters: file_keep_cache=%s direntry_cache_timeout=%.2f entry_cache_timeout=%.2f attr_cache_timeout=%.2f\n",(keep_cache==1)?"always":(keep_cache==2)?"never":"auto",direntry_cache_timeout,entry_cache_timeout,attr_cache_timeout);
		fprintf(stderr,"mkdir copy sgid=%d\nsugid clear mode=%s\n",mkdir_copy_sgid_,(sugid_clear_mode_<SUGID_CLEAR_MODE_OPTIONS)?sugid_clear_mode_strings[sugid_clear_mode_]:"???");
		fprintf(stderr, "ACL support %s\n", acl_enabled ? "enabled" : "disabled");
	}
	fs_statsptr_init();
}
