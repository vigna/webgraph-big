/*
 * Copyright (C) 2003-2022 Paolo Boldi and Sebastiano Vigna
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

import java.util.NoSuchElementException;

import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.webgraph.ImmutableGraph;

/** This interface extends {@link LongIterator} and is used to scan a graph, that is, to read its nodes and their successor lists
 *  sequentially. The {@link #nextLong()} method returns the node that will be scanned. After a call to this method,  calling
 *  {@link #successors()} or {@link #successorBigArray()} will return the list of successors.
 *
 *  <p>Implementing subclasses can override either {@link #successors()} or
 *  {@link #successorBigArray()}, but at least one of them <strong>must</strong> be implemented.
 */

public abstract class NodeIterator implements LongIterator {

	/**
	 * An empty node iterator.
	 */
	public static final NodeIterator EMPTY = new NodeIterator() {
		@Override
		public NodeIterator copy(final long upperBound) {
			return this;
		}

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public long outdegree() {
			throw new IllegalStateException();
		}

		@Override
		public long nextLong() {
			throw new NoSuchElementException();
		}
	};

	/** Returns the outdegree of the current node.
	 *
	 *  @return the outdegree of the current node.
	 */
	public abstract long outdegree();

	/**
	 * Returns a lazy iterator over the successors of the current node. The iteration terminates when -1
	 * is returned.
	 *
	 * @implSpec This implementation just wraps the array returned by {@link #successorBigArray()}.
	 *
	 * @return a lazy iterator over the successors of the current node.
	 */
	public LazyLongIterator successors() {
		return LazyLongIterators.wrap(successorBigArray(), outdegree());
	}

	/**
	 * Returns a reference to an array containing the successors of the current node.
	 *
	 * <P>
	 * The returned array may contain more entries than the outdegree of the current node. However, only
	 * those with indices from 0 (inclusive) to the outdegree of the current node (exclusive) contain
	 * valid data.
	 *
	 * @implSpec This implementation just unwrap the iterator returned by {@link #successors()}.
	 *
	 * @return an array whose first elements are the successors of the current node; the array must not
	 *         be modified by the caller.
	 */
	public long[][] successorBigArray() {
		final long[][] successor = LongBigArrays.newBigArray(outdegree());
		LazyLongIterators.unwrap(successors(), successor);
		return successor;
	}

	/**
	 * Creates a copy of this iterator that will never return nodes &ge; the specified bound; the copy
	 * must be accessible by a different thread. Optional operation (it should be implemented by all
	 * classes that allow to scan the graph more than once).
	 *
	 * @implSpec This implementation just throws an {@link UnsupportedOperationException}. It should be
	 *           kept in sync with the result of {@link ImmutableGraph#hasCopiableIterators()}.
	 *
	 * @param upperBound the upper bound.
	 * @return a copy of this iterator, with the given upper bound.
	 */
	public NodeIterator copy(final long upperBound) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Skips the given number of elements.
	 *
	 * <p>
	 * The effect of this call is exactly the same as that of calling {@link #nextLong()} for {@code n}
	 * times (possibly stopping if {@link #hasNext()} becomes false).
	 *
	 * <p>
	 * This method is a big version of {@link LongIterator#skip(int)}.
	 *
	 * @param n the number of elements to skip.
	 * @return the number of elements actually skipped.
	 * @see #nextLong()
	 */
	public long skip(final long n) {
		if (n < 0) throw new IllegalArgumentException("Argument must be nonnegative: " + n);
		long i = n;
		while (i-- != 0 && hasNext()) nextLong();
		return n - i - 1;
	}
}
