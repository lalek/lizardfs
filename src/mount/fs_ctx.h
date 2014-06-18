#pragma once
#include "config.h"

#include <sys/types.h>

struct fs_ctx {
	template <class Ctx>
	fs_ctx(const Ctx& t) : uid(t.uid), gid(t.gid), pid(t.pid), umask(t.umask) {
	}

	uid_t uid;
	gid_t gid;
	pid_t pid;
	mode_t umask;
};
