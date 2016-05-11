#ifndef _LUSTRE_COPYTOOL_CDEV_H
#define _LUSTRE_COPYTOOL_CDEV_H

/* Copytool char device ioctl */
#define CT_CDEV_IOC_MAGIC 0xC5
#define CT_CDEV_IOC_SET_ARCHIVE_MASK _IO(CT_CDEV_IOC_MAGIC, 1)
#define CT_CDEV_IOC_MAXNR 2

#endif
