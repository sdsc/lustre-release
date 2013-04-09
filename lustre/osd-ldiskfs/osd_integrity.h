/* GPL HEADER START
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
 * version 2 along with this program; If not, see http://www.gnu.org/licenses
 *
 * Please  visit http://www.xyratex.com/contact if you need additional
 * information or have any questions.
 *
 * GPL HEADER END
 */

/*
 * Copyright 2012 Xyratex Technology Limited
 *
 * Author: Andrew Perepechko <Andrew_Perepechko@xyratex.com>
 *
 */

int osd_alloc_integrity(struct osd_iobuf *iobuf, int pages);

int osd_setup_integrity(struct osd_device *osd, struct block_device *bdev);

int osd_check_integrity(struct osd_device *osd, struct page *page,
			struct integrity *integrity, int n);

int osd_pull_integrity(struct osd_device *osd,
		       struct niobuf_local *local_nb,
		       int npages,
		       struct integrity *integrity);

int osd_reconstruct_integrity(struct osd_device *osd,
			      struct niobuf_local *local_nb,
			      int npages,
			      struct integrity *integrity);

int osd_attach_integrity(struct osd_device *osd,
			 struct osd_iobuf *iobuf,
			 struct bio *bio, int page_idx,
			 unsigned start, unsigned end,
			 unsigned sector, int hole);

void osd_iobuf_attach_integrity(struct osd_iobuf *iobuf, struct page *page,
			        struct integrity *integrity, int n);
