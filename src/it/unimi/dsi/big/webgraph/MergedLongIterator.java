/*
 * Copyright (C) 2003-2021 Paolo Boldi and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.ints.IntIterator;

/** An iterator returning the union of the integers returned by two {@link IntIterator}s.
 *  The two iterators must return integers in an increasing fashion; the resulting
 *  {@link MergedLongIterator} will do the same. Duplicates will be eliminated.
 */

public class MergedLongIterator implements LazyLongIterator {
	/** The first component iterator. */
	private final LazyLongIterator it0;
	/** The second component iterator. */
	private final LazyLongIterator it1;
	/** The maximum number of integers to be still returned. */
	private long n;
	/** The last integer returned by {@link #it0}. */
	private long curr0;
	/** The last integer returned by {@link #it1}. */
	private long curr1;

	/** Creates a new merged iterator by merging two given iterators.
	 *
	 * @param it0 the first (monotonically nondecreasing) component iterator.
	 * @param it1 the second (monotonically nondecreasing) component iterator.
	 */
	public MergedLongIterator(final LazyLongIterator it0, final LazyLongIterator it1) {
		this (it0, it1, Integer.MAX_VALUE);
	}

	/** Creates a new merged iterator by merging two given iterators; the resulting iterator will not emit more than <code>n</code> integers.
	 *
	 * @param it0 the first (monotonically nondecreasing) component iterator.
	 * @param it1 the second (monotonically nondecreasing) component iterator.
	 * @param n the maximum number of integers this merged iterator will return.
	 */
	public MergedLongIterator(final LazyLongIterator it0, final LazyLongIterator it1, final long n) {
		this.it0 = it0;
		this.it1 = it1;
		this.n = n;
		curr0 = it0.nextLong();
		curr1 = it1.nextLong();
	}

	@Override
	public long nextLong() {
		if (n == 0 || curr0 == -1 && curr1 == -1) return -1;
		n--;

		final long result;

		if (curr0 == -1) {
			result = curr1;
			curr1 = it1.nextLong();
		}
		else if (curr1 == -1) {
			result = curr0;
			curr0 = it0.nextLong();
		}
		else if (curr0 < curr1) {
			result = curr0;
			curr0 = it0.nextLong();
		}
		else if (curr0 > curr1) {
			result = curr1;
			curr1 = it1.nextLong();
		}
		else {
			result = curr0;
			curr0 = it0.nextLong();
			curr1 = it1.nextLong();
		}

		return result;
	}

	@Override
	public long skip(final long s) {
		long i;
		for(i = 0; i < s; i++) {
			if (n == 0 || curr0 == -1 && curr1 == -1) break;
			n--;

			if (curr0 == -1) curr1 = it1.nextLong();
			else if (curr1 == -1) curr0 = it0.nextLong();
			else if (curr0 < curr1) curr0 = it0.nextLong();
			else if (curr0 > curr1) curr1 = it1.nextLong();
			else {
				curr0 = it0.nextLong();
				curr1 = it1.nextLong();
			}
		}
		return i;
	}
}
