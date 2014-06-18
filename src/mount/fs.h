#pragma once
#include "config.h"

#include "mount/fs_ctx.h"

// TODO mfs_meta

// TODO separate file fuse_interface.h|cc

// TODO comment public interfaces

#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <utility>
#include <vector>

typedef unsigned long int inode_t;

struct fs_file_info {
	template <class FileInfo>
	fs_file_info(FileInfo& t) : flags(t.flags), direct_io(t.direct_io),
			keep_cache(t.keep_cache), fh(t.fh) {
	};

	int flags;
	unsigned int direct_io : 1;
	unsigned int keep_cache : 1;
	uint64_t fh;
};

struct FsEntryParam {
	template <class EntryParam>
	FsEntryParam(const EntryParam& t) : ino(t.ino), generation(t.generation),
			attr(t.attr), attr_timeout(t.attr_timeout), entry_timeout(t.entry_timeout) {
	};
	FsEntryParam() : ino(0), generation(0), attr_timeout(0), entry_timeout(0) {
		memset(&attr, 0, sizeof(struct stat));
	}

	template <class T>
	T construct() {
		T ret;
		ret.ino           = ino;
		ret.generation    = generation;
		ret.attr          = attr;
		ret.attr_timeout  = attr_timeout;
		ret.entry_timeout = entry_timeout;
		return ret;
	}

	inode_t ino;
	unsigned long generation;
	struct stat attr;
	double attr_timeout;
	double entry_timeout;
};

// TODO fs_file_info* => fs_file_info&

struct fs {
//	TODO hmm
//	void init(void *userdata, struct fuse_conn_info *conn);

	FsEntryParam lookup(fs_ctx ctx, inode_t parent, const char *name);

	struct AttrReply {
		struct stat attr;
		double attrTimeout;
	};

	AttrReply getattr(fs_ctx ctx, inode_t ino, fs_file_info* fi);

	AttrReply setattr(fs_ctx ctx, inode_t ino, struct stat *attr,
			int to_set, fs_file_info* fi);
	std::string readlink(fs_ctx ctx, inode_t ino);

	FsEntryParam mknod(fs_ctx ctx, inode_t parent, const char *name, mode_t mode, dev_t rdev);

	FsEntryParam mkdir(fs_ctx ctx, inode_t parent, const char *name, mode_t mode);

	void unlink(fs_ctx ctx, inode_t parent, const char *name);

	void rmdir(fs_ctx ctx, inode_t parent, const char *name);

	FsEntryParam symlink(fs_ctx ctx, const char *link, inode_t parent,
			 const char *name);

	void rename(fs_ctx ctx, inode_t parent, const char *name,
			inode_t newparent, const char *newname);

	FsEntryParam link(fs_ctx ctx, inode_t ino, inode_t newparent, const char *newname);

	void open(fs_ctx ctx, inode_t ino, fs_file_info* fi);

	std::vector<uint8_t> read(fs_ctx ctx, inode_t ino, size_t size, off_t off,
			fs_file_info* fi);

	typedef size_t BytesWritten;
	BytesWritten write(fs_ctx ctx, inode_t ino, const char *buf, size_t size, off_t off,
			fs_file_info* fi);

	void flush(fs_ctx ctx, inode_t ino, fs_file_info* fi);

	void release(fs_ctx ctx, inode_t ino, fs_file_info* fi);

	void fsync(fs_ctx ctx, inode_t ino, int datasync, fs_file_info* fi);

	void opendir(fs_ctx ctx, inode_t ino, fs_file_info* fi);

	struct FsDirEntry {
		std::string name;
		struct stat stbuf;
		off_t off;
		size_t size;
	};
	std::vector<FsDirEntry> readdir(fs_ctx ctx, inode_t ino, size_t size, off_t off,
			fs_file_info* fi);

	void releasedir(fs_ctx ctx, inode_t ino, fs_file_info* fi);

	struct statvfs statfs(fs_ctx ctx, inode_t ino);

	void setxattr(fs_ctx ctx, inode_t ino, const char *name, const char *value,
			size_t size, int flags, uint32_t position);


	struct XattrReply {
		uint32_t valueLength;
		std::vector<uint8_t> valueBuffer;
	};

	XattrReply getxattr(fs_ctx ctx, inode_t ino, const char *name, size_t size, uint32_t position);

	XattrReply listxattr(fs_ctx ctx, inode_t ino, size_t size);

	void removexattr(fs_ctx ctx, inode_t ino, const char *name);

	void access(fs_ctx ctx, inode_t ino, int mask);

	FsEntryParam create(fs_ctx ctx, inode_t parent, const char *name,
			mode_t mode, fs_file_info* fi);




// TODO hmm
	void init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_,
			double entry_cache_timeout_, double attr_cache_timeout_, int mkdir_copy_sgid_,
			int sugid_clear_mode_, bool acl_enabled_);


// TODO chrum
	void remove_file_info(fs_file_info *f);
	void remove_dir_info(fs_file_info *f);








// TODO what about following fuse_lowlevel_ops functions:
// destroy
// forget
// fsyncdir
// getlk
// setlk
// bmap
// ioctl
// poll
// write_buf
// retrieve_reply
// forget_multi
// flock
// fallocate
// readdirplus

};
