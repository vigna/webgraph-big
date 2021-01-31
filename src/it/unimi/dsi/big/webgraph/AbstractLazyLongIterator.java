/*
 * Copyright (C) 2007-2021 Sebastiano Vigna
 *
 * This program and the accompanying materials are made available under the
 * terms of the GNU Lesser General Public License v2.1 or later,
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html,
 * or the Apache Software License 2.0, which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later OR Apache-2.0
 */

package it.unimi.dsi.big.webgraph;

/** An abstract implementation of a lazy integer iterator, implementing {@link #skip(long)}
 * by repeated calls to {@link LazyLongIterator#nextLong() nextInt()}. */

public abstract class AbstractLazyLongIterator implements LazyLongIterator {

	@Override
	public long skip(final long n) {
		long i;
		for(i = 0; i < n && nextLong() != -1; i++);
		return i;
	}

}
