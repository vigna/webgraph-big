/*
 * Copyright (C) 2007-2020 Paolo Boldi and Sebastiano Vigna
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

package it.unimi.dsi.big.webgraph.labelling;

import static it.unimi.dsi.fastutil.BigArrays.get;
import static it.unimi.dsi.fastutil.BigArrays.grow;
import static it.unimi.dsi.fastutil.BigArrays.set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.big.webgraph.Transform;
import it.unimi.dsi.big.webgraph.UnionImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectIterators;


/** An arc-labelled immutable graph representing the union of two given such graphs.
 *  Here by &ldquo;union&rdquo; we mean that an arc will belong to the union iff it belongs to at least one of the two graphs (the number of
 *  nodes of the union is taken to be the maximum among the number of nodes of each graph). Labels are assumed to have the same
 *  prototype in both graphs, and are treated as follows: if an arc is present in but one graph, its label in the resulting
 *  graph is going to be the label of the arc in the graph where it comes from; if an arc is present in both graphs, the labels
 *  are combined using a provided {@link LabelMergeStrategy}.
 *
 *  <h2>Remarks about the implementation</h2>
 *
 *  <p>Due to the lack of multiple inheritance, we could not extend both {@link UnionImmutableGraph}
 *  and {@link ArcLabelledImmutableGraph}, hence we forcedly decided to extend the latter. The possibility of using delegation
 *  on the former was also discarded because the code for reading and merging labels is so tightly coupled with the rest that it
 *  would have been essentially useless (and even dangerous) to delegate the iteration methods. As a result, some of the code of this
 *  class is actually almost a duplicate of the code of {@link UnionImmutableGraph}.
 */
public class UnionArcLabelledImmutableGraph extends ArcLabelledImmutableGraph {
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	private final ArcLabelledImmutableGraph g0, g1;
	private final long n0, n1, numNodes;

	/** The strategy used to merge labels when the same arc is present in both graphs. */
	private final LabelMergeStrategy labelMergeStrategy;

	/** The node whose successors are cached, or -1 if no successors are currently cached. */
	private final int cachedNode = -1;

	/** The outdegree of the cached node, if any. */
	private int outdegree;

	/** The successors of the cached node, if any; note that the array might be larger. */
	private long[][] cache = LongBigArrays.EMPTY_BIG_ARRAY;

	/**
	 * The labels on the arcs going out of the cached node, if any; note that the array might be larger.
	 */
	private Label labelCache[][] = Label.EMPTY_LABEL_BIG_ARRAY;
	/** The prototype for the labels of this graph. */
	private final Label prototype;

	@Override
	public UnionArcLabelledImmutableGraph copy() {
		return new UnionArcLabelledImmutableGraph(g0.copy(), g1.copy(), labelMergeStrategy);
	}

	/**
	 * Creates the union of two given graphs.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param labelMergeStrategy the strategy used to merge labels when the same arc is present in both
	 *            graphs.
	 */
	public UnionArcLabelledImmutableGraph(final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelMergeStrategy labelMergeStrategy) {
		this.g0 = g0;
		this.g1 = g1;
		this.labelMergeStrategy = labelMergeStrategy;
		n0 = g0.numNodes();
		n1 = g1.numNodes();
		numNodes = Math.max(n0, n1);
		if (g0.prototype().getClass() != g1.prototype().getClass()) throw new IllegalArgumentException("The two graphs have different label classes (" + g0.prototype().getClass().getSimpleName() + ", " + g1.prototype().getClass().getSimpleName() + ")");
		prototype = g0.prototype();
	}

	private static class InternalNodeIterator extends ArcLabelledNodeIterator {
		/** If outdegree is nonnegative, the successors of the current node (this array may be, however, larger). */
		@SuppressWarnings("hiding")
		private long cache[][];
		/** If outdegree is nonnegative, the labels on the arcs going out of the current node (this array may be, however, larger). */
		@SuppressWarnings("hiding")
		private Label[][] labelCache = Label.EMPTY_LABEL_BIG_ARRAY;
		/** The outdegree of the current node, or -1 if the successor array for the current node has not been computed yet. */
		@SuppressWarnings("hiding")
		private long outdegree = -1;
		private ArcLabelledNodeIterator i0;
		private ArcLabelledNodeIterator i1;
		private final LabelMergeStrategy labelMergeStrategy;

		public InternalNodeIterator(final ArcLabelledNodeIterator i0, final ArcLabelledNodeIterator i1, final LabelMergeStrategy labelMergeStrategy) {
			this(i0, i1, labelMergeStrategy, -1, LongBigArrays.EMPTY_BIG_ARRAY, Label.EMPTY_LABEL_BIG_ARRAY);
		}

		public InternalNodeIterator(final ArcLabelledNodeIterator i0, final ArcLabelledNodeIterator i1, final LabelMergeStrategy labelMergeStrategy, final long outdegree, final long[][] cache, final Label[][] labelCache) {
			this.i0 = i0;
			this.i1 = i1;
			this.labelMergeStrategy = labelMergeStrategy;
			this.outdegree = outdegree;
			this.cache = cache;
			this.labelCache = labelCache;
		}


		@Override
		public boolean hasNext() {
			return i0 != null && i0.hasNext() || i1 != null && i1.hasNext();
		}

		@Override
		public long nextLong() {
			if (! hasNext()) throw new java.util.NoSuchElementException();
			outdegree = -1;
			long result = -1;
			if (i0 != null) {
				if (i0.hasNext()) result = i0.nextLong();
				else i0 = null;
			}
			if (i1 != null) {
				if (i1.hasNext()) result = i1.nextLong();
				else i1 = null;
			}
			return result;
		}

		@Override
		public long[][] successorBigArray() {
			if (outdegree != -1) return cache;
			if (i0 == null) {
				outdegree = i1.outdegree();
				cache = i1.successorBigArray();
				labelCache = i1.labelBigArray();
				return cache;
			}
			if (i1 == null) {
				outdegree = i0.outdegree();
				cache = i0.successorBigArray();
				labelCache = i0.labelBigArray();
				return cache;
			}
			// We need to perform a manual merge
			final ArcLabelledNodeIterator.LabelledArcIterator succ0 = i0.successors();
			final ArcLabelledNodeIterator.LabelledArcIterator succ1 = i1.successors();
			long s0 = -1, s1 = -1;
			Label l0 = null, l1 = null;
			outdegree = 0;
			// Note that the parallel OR is necessary.
			while ((s0 != -1 || (s0 = succ0.nextLong()) != -1) | (s1 != -1 || (s1 = succ1.nextLong()) != -1)) {
				if (s0 != -1) l0 = succ0.label().copy();
				if (s1 != -1) l1 = succ1.label().copy();
				assert s0 >= 0 || s1 >= 0;
				cache = grow(cache, outdegree + 1);
				labelCache = grow(labelCache, outdegree + 1);
				if (s1 < 0 || 0 <= s0 && s0 < s1) {
					set(cache, outdegree, s0);
					set(labelCache, outdegree, l0);
					s0 = -1;
				} else if (s0 < 0 || 0 <= s1 && s1 < s0) {
					set(cache, outdegree, s1);
					set(labelCache, outdegree, l1);
					s1 = -1;
				} else {
					assert s0 == s1 && s0 >= 0;
					set(cache, outdegree, s0);
					set(labelCache, outdegree, labelMergeStrategy.merge(l0, l1));
					s0 = s1 = -1;
				}
				outdegree++;
			}
			return cache;
		}

		@Override
		public long outdegree() {
			successorBigArray(); // So that the cache is filled up
			return outdegree;
		}

		@Override
		public Label[][] labelBigArray() {
			successorBigArray(); // So that the cache is filled up
			return labelCache;
		}

		@Override
		public LabelledArcIterator successors() {
			successorBigArray(); // So that the cache is filled up
			return new LabelledArcIterator() {
				long nextToBeReturned = 0;

				@Override
				public Label label() {
					return get(labelCache, nextToBeReturned - 1);
				}

				@Override
				public long nextLong() {
					if (nextToBeReturned == outdegree) return -1;
					return get(cache, nextToBeReturned++);
				}

				@Override
				public long skip(final long x) {
					final long skipped = Math.min(x, outdegree - nextToBeReturned);
					nextToBeReturned += skipped;
					return skipped;
				}
			};

		}


		@Override
		public final ArcLabelledNodeIterator copy(final long upperBound) {
			return new InternalNodeIterator(i0 == null ? i0 : i0.copy(upperBound), i1 == null ? i1 : i1.copy(upperBound), labelMergeStrategy, outdegree, BigArrays.copy(cache, 0, Math.max(0, outdegree)), BigArrays.copy(labelCache, 0, Math.max(0, outdegree)));
		}
	}

	@Override
	public ArcLabelledNodeIterator nodeIterator(final long from) {
		return new InternalNodeIterator(from < n0 ? g0.nodeIterator(from) : null, from < n1 ? g1.nodeIterator(from) : null, labelMergeStrategy);
	}

	@Override
	public long numNodes() {
		return numNodes;
	}

	@Override
	public boolean randomAccess() {
		return g0.randomAccess() && g1.randomAccess();
	}

	@Override
	public boolean hasCopiableIterators() {
		return g0.hasCopiableIterators() && g1.hasCopiableIterators();
	}

	@Override
	public long[][] successorBigArray(final long x) {
		if (x == cachedNode) return cache;
		// We need to perform a manual merge
		final ArcLabelledNodeIterator.LabelledArcIterator succ0 = (LabelledArcIterator) (x < n0? g0.successors(x) : ObjectIterators.EMPTY_ITERATOR);
		final ArcLabelledNodeIterator.LabelledArcIterator succ1 = (LabelledArcIterator) (x < n1? g1.successors(x) : ObjectIterators.EMPTY_ITERATOR);
		long outdegree = 0;
		long s0 = -1, s1 = -1;
		Label l0 = null, l1 = null;
		while ((s0 != -1 || (s0 = succ0.nextLong()) != -1) | (s1 != -1 || (s1 = succ1.nextLong()) != -1)) {
			if (s0 != -1) l0 = succ0.label().copy();
			if (s1 != -1) l1 = succ1.label().copy();
			assert s0 >= 0 || s1 >= 0;
			cache = BigArrays.grow(cache, outdegree + 1);
			labelCache = grow(labelCache, outdegree + 1);
			if (s1 < 0 || 0 <= s0 && s0 < s1) {
				set(cache, outdegree, s0);
				set(labelCache, outdegree, l0);
				s0 = -1;
			} else if (s0 < 0 || 0 <= s1 && s1 < s0) {
				set(cache, outdegree, s1);
				set(labelCache, outdegree, l1);
				s1 = -1;
			} else {
				assert s0 == s1 && s0 >= 0;
				set(cache, outdegree, s0);
				set(labelCache, outdegree, labelMergeStrategy.merge(l0, l1));
				s0 = s1 = -1;
			}
			outdegree++;
		}
		return cache;
	}

	@Override
	public long outdegree(final long x) {
		successorBigArray(x); // So the cache gets filled
		return outdegree;
	}

	@Override
	public Label[][] labelBigArray(final long x) {
		successorBigArray(x); // So that the cache is filled up
		return labelCache;
	}

	@Override
	public LabelledArcIterator successors(final long x) {
		successorBigArray(x); // So that the cache is filled up
		return new LabelledArcIterator() {
			long nextToBeReturned = 0;

			@Override
			public Label label() {
				return get(labelCache, nextToBeReturned);
			}

			@Override
			public long nextLong() {
				if (nextToBeReturned == outdegree) return -1;
				return get(cache, nextToBeReturned++);
			}

			@Override
			public long skip(final long x) {
				final long skipped = Math.min(x, outdegree - nextToBeReturned);
				nextToBeReturned += skipped;
				return skipped;
			}
		};
	}

	@Override
	public Label prototype() {
		return prototype;
	}

}
