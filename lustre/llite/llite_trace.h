/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2016, James Simmons <jsimmons@infradead.org>
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Seagate, Inc.
 *
 * lustre/llite/llite_trace.h
 *
 * llite tracepoint handling
 */

#undef TRACE_SYSTEM
#define TRACE_SYSTEM llite

#ifndef DEBUG_SUBSYSTEM
#define DEBUG_SUBSYSTEM S_LLITE
#endif

#if !defined(__LLITE_TRACE_H) || defined(TRACE_HEADER_MULTI_READ)
#define __LLITE_TRACE_H

#include <linux/limits.h>
#include <linux/tracepoint.h>
#include <libcfs/libcfs_debug.h>

DECLARE_EVENT_CLASS(llite_log_msg,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf),
	TP_STRUCT__entry(
		__field(const char *, msg_file)
		__field(int, msg_line)
		__field(const char *, msg_fn)
		__dynamic_array(char, msg, CFS_TRACE_CONSOLE_BUFFER_SIZE)
	),
	TP_fast_assign(
		__entry->msg_file = msg_file;
		__entry->msg_line = msg_line;
		__entry->msg_fn = msg_fn;
		vsnprintf(__get_dynamic_array(msg),
			  CFS_TRACE_CONSOLE_BUFFER_SIZE, vaf->fmt, *vaf->va);
	),
	TP_printk("(%s:%d:%s) %s", __entry->msg_file, __entry->msg_line,
		  __entry->msg_fn, __get_str(msg))
);

DEFINE_EVENT(llite_log_msg, llite_info,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_config,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_other,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_ioctl,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_ha,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_rpctrace,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_dlmtrace,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_vfstrace,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_super,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_dentry,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_inode,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_cache,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_page,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_mmap,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_reada,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_hsm,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(llite_log_msg, llite_sec,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

#endif /* __LLITE_TRACE_H */

#undef TRACE_INCLUDE_PATH
#define TRACE_INCLUDE_PATH ../llite

#undef TRACE_INCLUDE_FILE
#define TRACE_INCLUDE_FILE llite_trace

#include <trace/define_trace.h>
