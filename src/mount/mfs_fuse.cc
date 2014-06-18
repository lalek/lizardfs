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
#include "mount/mfs_fuse.h"

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
#include <fuse/fuse_lowlevel.h>

#include "common/access_control_list.h"
#include "common/acl_converter.h"
#include "common/acl_type.h"
#include "common/datapack.h"
#include "common/MFSCommunication.h"
#include "common/posix_acl_xattr.h"
#include "common/strerr.h"
#include "common/time_utils.h"
#include "mount/dirattrcache.h"
#include "mount/fs.h"
#include "mount/fs_ctx.h"
#include "mount/g_io_limiters.h"
#include "mount/io_limit_group.h"
#include "mount/mastercomm.h"
#include "mount/masterproxy.h"
#include "mount/oplog.h"
#include "mount/readdata.h"
#include "mount/stats.h"
#include "mount/symlinkcache.h"
#include "mount/writedata.h"

#if MFS_ROOT_ID != FUSE_ROOT_ID
#error FUSE_ROOT_ID is not equal to MFS_ROOT_ID
#endif

#define READDIR_BUFFSIZE 50000

// TODO std::bad_alloc

static fs lizardfs;

fs_ctx get_fs_ctx(fuse_req_t& req) {
	return fs_ctx(*fuse_req_ctx(req));
}

class wrap_fuse_file_info {
public:
	wrap_fuse_file_info(fuse_file_info* fi) : fuse_fi_(fi),
			fs_fi_(fuse_fi_ ? new fs_file_info(*fuse_fi_) : nullptr){
	}
	operator fs_file_info*() {
		return fs_fi_.get();
	}
	~wrap_fuse_file_info() {
		if (fs_fi_) {
			sassert(fuse_fi_);
			fuse_fi_->direct_io  = fs_fi_->direct_io;
			fuse_fi_->fh         = fs_fi_->fh;
			fuse_fi_->flags      = fs_fi_->flags;
			fuse_fi_->keep_cache = fs_fi_->keep_cache;
		} else {
			sassert(!fuse_fi_);
		}
	}

private:
	fuse_file_info* fuse_fi_;
	std::unique_ptr<fs_file_info> fs_fi_;
};

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

#if FUSE_USE_VERSION >= 26
void mfs_statfs(fuse_req_t req,fuse_ino_t ino) {
#else
void mfs_statfs(fuse_req_t req) {
	fuse_ino_t ino = 0;
#endif
	try {
		auto a = lizardfs.statfs(get_fs_ctx(req), ino);
		fuse_reply_statfs(req, &a);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_access(fuse_req_t req, fuse_ino_t ino, int mask) {
	try {
		lizardfs.access(get_fs_ctx(req), ino, mask);
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
	try {
		auto fuseEntryParam = lizardfs.lookup(get_fs_ctx(req), parent, name)
				.construct<fuse_entry_param>();
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		auto a = lizardfs.getattr(get_fs_ctx(req), ino, wrap_fuse_file_info(fi));
		fuse_reply_attr(req, &a.attr, a.attrTimeout);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *stbuf, int to_set, struct fuse_file_info *fi) {
	try {
		auto a = lizardfs.setattr(get_fs_ctx(req), ino, stbuf, to_set, wrap_fuse_file_info(fi));
		fuse_reply_attr(req, &a.attr, a.attrTimeout);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
	try {
		auto fuseEntryParam = lizardfs.mknod(get_fs_ctx(req), parent, name, mode, rdev)
				.construct<fuse_entry_param>();
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
	try {
		lizardfs.unlink(get_fs_ctx(req), parent, name);
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode) {
	try {
		auto fuseEntryParam = lizardfs.mkdir(get_fs_ctx(req), parent, name, mode)
				.construct<fuse_entry_param>();
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
	try {
		lizardfs.rmdir(get_fs_ctx(req), parent, name);
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_symlink(fuse_req_t req, const char *path, fuse_ino_t parent, const char *name) {
	try {
		auto fuseEntryParam = lizardfs.symlink(get_fs_ctx(req), path, parent, name)
				.construct<fuse_entry_param>();
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_readlink(fuse_req_t req, fuse_ino_t ino) {
	try {
		fuse_reply_readlink(req,
				lizardfs.readlink(get_fs_ctx(req), ino).c_str());
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname) {
	try {
		lizardfs.rename(get_fs_ctx(req), parent, name, newparent, newname);
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname) {
	try {
		auto fuseEntryParam = lizardfs.link(get_fs_ctx(req), ino, newparent, newname)
				.construct<fuse_entry_param>();
		fuse_reply_entry(req, &fuseEntryParam);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		lizardfs.opendir(get_fs_ctx(req), ino, wrap_fuse_file_info(fi));
		if (fuse_reply_open(req, fi) == -ENOENT) {
			lizardfs.remove_dir_info(wrap_fuse_file_info(fi));
		}
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	try {
		auto fsDirEntries =
				lizardfs.readdir(get_fs_ctx(req), ino, size, off, wrap_fuse_file_info(fi));

		if (fsDirEntries.empty()) {
			fuse_reply_buf(req, NULL, 0);
			return;
		}

		char buffer[READDIR_BUFFSIZE];
		size_t opos, oleng;
		opos = 0;
		for (auto& e : fsDirEntries) {
			oleng = fuse_add_direntry(req, buffer + opos, e.size - opos, e.name.c_str(),
					&(e.stbuf), e.off);
			if (opos + oleng > size) {
				break;
			} else {
				opos += oleng;
			}
		}
		fuse_reply_buf(req, buffer, opos);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		lizardfs.releasedir(get_fs_ctx(req), ino, wrap_fuse_file_info(fi));
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi) {
	try {
		auto tmp = lizardfs.create(get_fs_ctx(req), parent, name, mode, wrap_fuse_file_info(fi));
		auto e = tmp.construct<fuse_entry_param>();
		if (fuse_reply_create(req, &e, fi) == -ENOENT) {
			lizardfs.remove_file_info(wrap_fuse_file_info(fi));
		}
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		lizardfs.open(get_fs_ctx(req), ino, wrap_fuse_file_info(fi));
		if (fuse_reply_open(req, fi) == -ENOENT) {
			lizardfs.remove_file_info(wrap_fuse_file_info(fi));
		}
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		lizardfs.release(get_fs_ctx(req), ino, wrap_fuse_file_info(fi));
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
	try {
		auto ret = lizardfs.read(get_fs_ctx(req), ino, size, off, wrap_fuse_file_info(fi));
		if (ret.empty()) {
			fuse_reply_buf(req, NULL, 0);
		} else {
			fuse_reply_buf(req, (char*) ret.data(), ret.size());
		}
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
	try {
		fuse_reply_write(req,
				lizardfs.write(get_fs_ctx(req), ino, buf, size, off, wrap_fuse_file_info(fi)));
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
	try {
		lizardfs.flush(get_fs_ctx(req), ino, wrap_fuse_file_info(fi));
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
	try {
		lizardfs.fsync(get_fs_ctx(req), ino, datasync, wrap_fuse_file_info(fi));
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

#if defined(__APPLE__)
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags, uint32_t position) {
#else
void mfs_setxattr (fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags) {
	uint32_t position=0;
#endif
	try {
		lizardfs.setxattr(get_fs_ctx(req), ino, name, value, size, flags, position);
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

#if defined(__APPLE__)
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size, uint32_t position) {
#else
void mfs_getxattr (fuse_req_t req, fuse_ino_t ino, const char *name, size_t size) {
	uint32_t position=0;
#endif /* __APPLE__ */
	try {
		auto a = lizardfs.getxattr(get_fs_ctx(req), ino, name, size, position);
		if (size == 0) {
			fuse_reply_xattr(req, a.valueLength);
		} else {
			fuse_reply_buf(req,(const char*)a.valueBuffer.data(), a.valueLength);
		}
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_listxattr (fuse_req_t req, fuse_ino_t ino, size_t size) {
	try {
		auto a = lizardfs.listxattr(get_fs_ctx(req), ino, size);
		if (size == 0) {
			fuse_reply_xattr(req, a.valueLength);
		} else {
			fuse_reply_buf(req,(const char*)a.valueBuffer.data(), a.valueLength);
		}
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_removexattr (fuse_req_t req, fuse_ino_t ino, const char *name) {
	try {
		lizardfs.removexattr(get_fs_ctx(req), ino, name);
		fuse_reply_err(req, 0);
	} catch (int i) {
		fuse_reply_err(req, i);
	}
}

void mfs_init(int debug_mode_, int keep_cache_, double direntry_cache_timeout_,
		double entry_cache_timeout_, double attr_cache_timeout_, int mkdir_copy_sgid_,
		int sugid_clear_mode_, bool acl_enabled_) {
	// FIXME
	lizardfs.init(debug_mode_, keep_cache_, direntry_cache_timeout_, entry_cache_timeout_, attr_cache_timeout_,
			mkdir_copy_sgid_, sugid_clear_mode_, acl_enabled_);
}
