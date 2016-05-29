#ifndef __LUSTRE_COPYTOOL_CDEV_H__
#define __LUSTRE_COPYTOOL_CDEV_H__

/* Copytool char device ioctl */
#define CT_CDEV_IOC_MAGIC 0xC5
#define CT_CDEV_IOC_SET_ARCHIVE_MASK _IO(CT_CDEV_IOC_MAGIC, 1)
#define CT_CDEV_IOC_MAXNR 2

struct ct_cdev;

#endif
