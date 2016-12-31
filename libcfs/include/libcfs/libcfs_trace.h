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
 * libcfs/include/libcfs_trace.h
 *
 * LNet/libcfs tracepoint handling
 */

#undef TRACE_SYSTEM
#define TRACE_SYSTEM lnet

#define DEBUG_SUBSYSTEM S_LNET

#if !defined(__LIBCFS_TRACE_H) || defined(TRACE_HEADER_MULTI_READ)
#define __LIBCFS_TRACE_H

#include <linux/limits.h>
#include <linux/tracepoint.h>
#include <libcfs/libcfs_debug.h>

#define CFS_TRACE_CONSOLE_BUFFER_SIZE	1024

DECLARE_EVENT_CLASS(lnet_log_msg,
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

DEFINE_EVENT(lnet_log_msg, lnet_info,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(lnet_log_msg, lnet_config,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(lnet_log_msg, lnet_other,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

DEFINE_EVENT(lnet_log_msg, lnet_ioctl,
	TP_PROTO(const char *msg_file, int msg_line, const char *msg_fn,
		 struct va_format *vaf),
	TP_ARGS(msg_file, msg_line, msg_fn, vaf)
);

#define trace_info(fmt, ...)						\
do {									\
	libcfs_debug_trace(trace_lnet_info, __FILE__, __LINE__,		\
			   __func__, fmt, ## __VA_ARGS__);		\
									\
	if (cfs_cdebug_show(D_INFO, DEBUG_SUBSYSTEM)) {			\
		static struct libcfs_debug_msg_data msgdata;		\
									\
		LIBCFS_DEBUG_MSG_DATA_INIT(&msgdata, D_INFO, NULL);	\
		libcfs_debug_msg(&msgdata, fmt, ## __VA_ARGS__);	\
	}								\
} while (0)

#define trace_config(fmt, ...)						\
do {									\
	libcfs_debug_trace(trace_lnet_config, __FILE__, __LINE__,	\
			   __func__, fmt, ## __VA_ARGS__);		\
									\
	if (cfs_cdebug_show(D_CONFIG, DEBUG_SUBSYSTEM)) {		\
		static struct libcfs_debug_msg_data msgdata;		\
									\
		LIBCFS_DEBUG_MSG_DATA_INIT(&msgdata, D_CONFIG, NULL);	\
		libcfs_debug_msg(&msgdata, fmt, ## __VA_ARGS__);	\
	}								\
} while (0)

#define trace_other(fmt, ...)						\
do {									\
	libcfs_debug_trace(trace_lnet_other, __FILE__, __LINE__,	\
			   __func__, fmt, ## __VA_ARGS__);		\
									\
	if (cfs_cdebug_show(D_OTHER, DEBUG_SUBSYSTEM)) {		\
		static struct libcfs_debug_msg_data msgdata;		\
									\
		LIBCFS_DEBUG_MSG_DATA_INIT(&msgdata, D_OTHER, NULL);	\
		libcfs_debug_msg(&msgdata, fmt, ## __VA_ARGS__);	\
	}								\
} while (0)

#define trace_ioctl(fmt, ...)						\
do {									\
	libcfs_debug_trace(trace_lnet_ioctl, __FILE__, __LINE__,	\
			   __func__, fmt, ## __VA_ARGS__);		\
									\
	if (cfs_cdebug_show(D_IOCTL, DEBUG_SUBSYSTEM)) {		\
		static struct libcfs_debug_msg_data msgdata;		\
									\
		LIBCFS_DEBUG_MSG_DATA_INIT(&msgdata, D_IOCTL, NULL);	\
		libcfs_debug_msg(&msgdata, fmt, ## __VA_ARGS__);	\
	}								\
} while (0)

#endif /* __LIBCFS_TRACE_H */

#undef TRACE_INCLUDE_PATH
#define TRACE_INCLUDE_PATH libcfs

#undef TRACE_INCLUDE_FILE
#define TRACE_INCLUDE_FILE libcfs_trace

#include <trace/define_trace.h>
