/*
 * Copyright (C) 2013-2022 Sebastiano Vigna
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

/** A skippable {@linkplain LazyLongIterator lazy iterator over longs}.
 *
 * <p>An instance of this class represent an iterator over longs
 * that returns elements in increasing order. The iterator makes it possible to {@linkplain #skip(long) skip elements
 * by <em>value</em>}.
 */

public interface LazyLongSkippableIterator extends LazyLongIterator {
	public static final long END_OF_LIST = Long.MAX_VALUE;

	/** Skips to a given element.
	 *
	 * <p>Note that this interface is <em>fragile</em>: after {@link #END_OF_LIST}
	 * has been returned, the behavour of further calls to this method will be
	 * unpredictable.
	 *
	 * @param lowerBound a lower bound to the returned element.
	 * @return if the last returned element is greater than or equal to
	 * {@code lowerBound}, the last returned element; otherwise,
	 * the smallest element greater
	 * than or equal to <code>lowerBound</code> that would be
	 * returned by this iterator, or {@link #END_OF_LIST}
	 * if no such element exists.
	 */
	public long skipTo(long lowerBound);
}
