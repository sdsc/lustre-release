/*
 * LGPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
 *
 * LGPL HEADER END
 *
 * Copyright (c) 2015, James Simmons <jsimmons@infradead.org>
 */

#ifndef LIB_LND_CONFIG_API_H
#define LIB_LND_CONFIG_API_H

#include <lnet/lib-dlc.h>
#include "cyaml.h"

int
lustre_ko2iblnd_show_net(struct cYAML *lndparams,
			 struct lnet_lnd_tunables *lnd_cfg);
void
lustre_ko2iblnd_parse_net(struct cYAML *lndparams,
			  struct lnet_lnd_tunables *lnd_cfg);

#endif /* LIB_LND_CONFIG_API_H */
