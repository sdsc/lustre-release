#include <sys/param.h>
#include <sys/kernel.h>
#include <sys/malloc.h>
#include <sys/systm.h>
#include <sys/sysctl.h>
#include <sys/proc.h>
#include <sys/unistd.h>
#include <mach/mach_types.h>
#include <linux/lustre_build_version.h>

#define DEBUG_SUBSYSTEM S_CLASS
                                                                                                                                                                     
#include <libcfs/libcfs.h>
#ifndef BUILD_VERSION	
#define BUILD_VERSION		"Unknown"
#endif
#ifndef LUSTRE_KERNEL_VERSION
#define LUSTRE_KERNEL_VERSION	"Unknown Darwin version"
#endif

cfs_sysctl_table_header_t *obd_table_header = NULL;

int proc_fail_loc SYSCTL_HANDLER_ARGS;
int proc_obd_timeout SYSCTL_HANDLER_ARGS;
extern unsigned int obd_fail_loc;
extern unsigned int obd_dump_on_timeout;
extern unsigned int obd_timeout;
extern unsigned int ldlm_timeout;
extern unsigned int obd_sync_filter;
extern atomic_t obd_memory;

int read_build_version SYSCTL_HANDLER_ARGS;
int read_lustre_kernel_version SYSCTL_HANDLER_ARGS;

SYSCTL_NODE (,                  OID_AUTO,       lustre,	    CTLFLAG_RW,
	     0,                 "lustre sysctl top");
SYSCTL_PROC(_lustre,		OID_AUTO,       fail_loc, 
	    CTLTYPE_INT | CTLFLAG_RW ,		&obd_fail_loc,
	    0,		&proc_fail_loc,		"I",	"obd_fail_loc");
SYSCTL_PROC(_lustre,		OID_AUTO,       timeout, 
	    CTLTYPE_INT | CTLFLAG_RW ,		&obd_timeout,
	    0,		&proc_obd_timeout,	"I",	"obd_timeout");
SYSCTL_PROC(_lustre,		OID_AUTO,       build_version, 
	    CTLTYPE_STRING | CTLFLAG_RD ,	NULL,
	    0,		&read_build_version,	"A",	"lustre_build_version");
SYSCTL_PROC(_lustre,		OID_AUTO,       lustre_kernel_version,
	    CTLTYPE_STRING | CTLFLAG_RD ,	NULL,
	    0,		&read_lustre_kernel_version,	"A",	"lustre_build_version");
SYSCTL_INT(_lustre,		OID_AUTO,	dump_on_timeout, 
	   CTLTYPE_INT | CTLFLAG_RW,		&obd_dump_on_timeout,
	   0,		"lustre_dump_on_timeout");
SYSCTL_INT(_lustre,		OID_AUTO,	memused, 
	   CTLTYPE_INT | CTLFLAG_RW,		(int *)&obd_memory.counter,
	   0,		"lustre_memory_used");
SYSCTL_INT(_lustre,		OID_AUTO,	ldlm_timeout, 
	   CTLTYPE_INT | CTLFLAG_RW,		&ldlm_timeout,
	   0,		"ldlm_timeout");

static cfs_sysctl_table_t      parent_table[] = {
	&sysctl__lustre,
	&sysctl__lustre_fail_loc,
	&sysctl__lustre_timeout,
	&sysctl__lustre_dump_on_timeout,
	&sysctl__lustre_upcall,
	&sysctl__lustre_memused,
	&sysctl__lustre_filter_sync_on_commit,
	&sysctl__lustre_ldlm_timeout,
};

extern cfs_waitq_t obd_race_waitq;

int proc_fail_loc SYSCTL_HANDLER_ARGS
{ 
	int error = 0; 
	int old_fail_loc = obd_fail_loc;
	
	error = sysctl_handle_long(oidp, oidp->oid_arg1, oidp->oid_arg2, req); 
	if (!error && req->newptr != USER_ADDR_NULL) {
		if (old_fail_loc != obd_fail_loc) 
			cfs_waitq_signal(&obd_race_waitq);
	} else  if (req->newptr != USER_ADDR_NULL) { 
		/* Something was wrong with the write request */ 
		printf ("sysctl fail loc fault: %d.\n", error);
	} else { 
		/* Read request */ 
		error = SYSCTL_OUT(req, &obd_fail_loc, sizeof obd_fail_loc);
	}
	return error;
}

int proc_obd_timeout SYSCTL_HANDLER_ARGS
{ 
	int error = 0;

	error = sysctl_handle_long(oidp, oidp->oid_arg1, oidp->oid_arg2, req); 
	if (!error && req->newptr != USER_ADDR_NULL) {
		if (ldlm_timeout >= obd_timeout)
			ldlm_timeout = max(obd_timeout / 3, 1U);
	} else  if (req->newptr != USER_ADDR_NULL) { 
		printf ("sysctl fail obd_timeout: %d.\n", error);
	} else {
		/* Read request */ 
		error = SYSCTL_OUT(req, &obd_timeout, sizeof obd_timeout);
	}
	return error;
}

int read_build_version SYSCTL_HANDLER_ARGS
{
	int error = 0;

	error = sysctl_handle_long(oidp, oidp->oid_arg1, oidp->oid_arg2, req); 
	if ( req->newptr != USER_ADDR_NULL) {
		printf("sysctl read_build_version is read-only!\n");
	} else {
		error = SYSCTL_OUT(req, BUILD_VERSION, strlen(BUILD_VERSION));
	}
	return error;
}

int read_lustre_kernel_version SYSCTL_HANDLER_ARGS
{
	int error = 0;

	error = sysctl_handle_long(oidp, oidp->oid_arg1, oidp->oid_arg2, req); 
	if ( req->newptr != NULL) {
		printf("sysctl lustre_kernel_version is read-only!\n");
	} else {
		error = SYSCTL_OUT(req, LUSTRE_KERNEL_VERSION, strlen(LUSTRE_KERNEL_VERSION));
	}
	return error;
}

void obd_sysctl_init (void)
{
#if 1 
	if ( !obd_table_header ) 
		obd_table_header = cfs_register_sysctl_table(parent_table, 0);
#endif
}
                                                                                                                                                                     
void obd_sysctl_clean (void)
{
#if 1 
	if ( obd_table_header ) 
		cfs_unregister_sysctl_table(obd_table_header); 
	obd_table_header = NULL;
#endif
}

