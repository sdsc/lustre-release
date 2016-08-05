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
 * http://www.sun.com/software/products/lustre/docs/GPLv2.pdf
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2016, Commissariat a l'Energie Atomique et aux Energies
 *                     Alternatives.
 */

#ifndef _LUSTRE_COPYTOOL_CDEV_H
#define _LUSTRE_COPYTOOL_CDEV_H

/* Copytool char device ioctl */
#define CT_CDEV_IOC_MAGIC 0xC5
#define CT_CDEV_IOC_SET_ARCHIVE_MASK _IO(CT_CDEV_IOC_MAGIC, 1)
#define CT_CDEV_IOC_MAXNR 2

#endif
