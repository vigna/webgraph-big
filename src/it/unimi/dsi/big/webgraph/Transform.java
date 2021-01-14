/*
 * Copyright (C) 2003-2020 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import static it.unimi.dsi.fastutil.BigArrays.ensureCapacity;
import static it.unimi.dsi.fastutil.BigArrays.get;
import static it.unimi.dsi.fastutil.BigArrays.grow;
import static it.unimi.dsi.fastutil.BigArrays.length;
import static it.unimi.dsi.fastutil.BigArrays.set;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableSequentialGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.Label;
import it.unimi.dsi.big.webgraph.labelling.LabelMergeStrategy;
import it.unimi.dsi.big.webgraph.labelling.LabelSemiring;
import it.unimi.dsi.big.webgraph.labelling.Labels;
import it.unimi.dsi.big.webgraph.labelling.UnionArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;


/** Static methods that manipulate immutable graphs.
 *
 *  <P>Most methods take an {@link
 *  it.unimi.dsi.big.webgraph.ImmutableGraph} (along with some other data, that
 *  depend on the kind of transformation), and return another {@link
 *  it.unimi.dsi.big.webgraph.ImmutableGraph} that represents the transformed
 *  version.
 */

public class Transform {
	private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);

	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	private Transform() {}


	/** Provides a method to accept or reject an arc.
	 *
	 * <P>Note that arc filters are usually stateless. Thus, their declaration
	 * should comprise a static singleton (e.g., {@link Transform#NO_LOOPS}).
	 */
	public interface ArcFilter {

		/**
		 * Tells if the arc <code>(i,j)</code> has to be accepted or not.
		 *
		 * @param i the source of the arc.
		 * @param t the destination of the arc.
		 * @return if the arc has to be accepted.
		 */
		public boolean accept(long i, long t);
	}

	/** Provides a method to accept or reject a labelled arc.
	 *
	 * <P>Note that arc filters are usually stateless. Thus, their declaration
	 * should comprise a static singleton (e.g., {@link Transform#NO_LOOPS}).
	 */
	public interface LabelledArcFilter {

		/**
		 * Tells if the arc <code>(i,j)</code> with label <code>label</code> has to be accepted or not.
		 *
		 * @param i the source of the arc.
		 * @param j the destination of the arc.
		 * @param label the label of the arc.
		 * @return if the arc has to be accepted.
		 */
		public boolean accept(long i, long j, Label label);
	}

	/** An arc filter that rejects loops. */
	final static private class NoLoops implements ArcFilter, LabelledArcFilter {
		private NoLoops() {}
		/** Returns true if the two arguments differ.
		 *
		 * @return <code>i != j</code>.
		 */
		@Override
		public boolean accept(final long i, final long j) {
			return i != j;
		}
		@Override
		public boolean accept(final long i, final long j, final Label label) {
			return i != j;
		}
	}

	/** An arc filter that only accepts arcs whose endpoints belong to the same
	 * (if the parameter <code>keepOnlySame</code> is true) or to
	 *  different (if <code>keepOnlySame</code> is false) classes.
	 *  Classes are specified by one long per node, read from a given file in {@link DataInput} format. */
	public final static class NodeClassFilter implements ArcFilter, LabelledArcFilter {
		private final boolean keepOnlySame;
		private final long[][] nodeClass;

		/** Creates a new instance.
		 *
		 * @param classFile name of the class file.
		 * @param keepOnlySame whether to keep nodes in the same class.
		 */
		public NodeClassFilter(final String classFile, final boolean keepOnlySame) {
			try {
				nodeClass = BinIO.loadLongsBig(classFile);
			}
			catch (final IOException e) {
				throw new RuntimeException(e);
			}
			this.keepOnlySame = keepOnlySame;
		}

		/** Creates a new instance.
		 *
		 * <p>This constructor has the same arguments as {@link #NodeClassFilter(String,boolean)},
		 * but it can be used with an {@link ObjectParser}.
		 *
		 * @param classFile name of the class file.
		 * @param keepOnlySame whether to keep nodes in the same class.
		 */
		public NodeClassFilter(final String classFile, final String keepOnlySame) {
			this(classFile, Boolean.parseBoolean(keepOnlySame));
		}

		@Override
		public boolean accept(final long i, final long j) {
			return keepOnlySame == (get(nodeClass, i) == get(nodeClass, j));
		}

		@Override
		public boolean accept(final long i, final long j, final Label label) {
			return keepOnlySame == (get(nodeClass, i) == get(nodeClass, j));
		}
	}

	/** An arc filter that rejects arcs whose well-known attribute has a value smaller than a given threshold. */
	final static public class LowerBound implements LabelledArcFilter {
		private final int lowerBound;

		public LowerBound(final int lowerBound) {
			this.lowerBound = lowerBound;
		}

		public LowerBound(final String lowerBound) {
			this(Integer.parseInt(lowerBound));
		}
		/** Returns true if the integer value associated to the well-known attribute of the label is larger than the threshold.
		 *
		 * @return true if <code>label.{@link Label#getInt()}</code> is larger than the threshold.
		 */
		@Override
		public boolean accept(final long i, final long j, final Label label) {
			return label.getInt() >= lowerBound;
		}
	}


	/** A singleton providing an arc filter that rejects loops. */
	final static public NoLoops NO_LOOPS = new NoLoops();

	/** A class that exposes an immutable graph viewed through a filter. */
	private static final class FilteredImmutableGraph extends ImmutableGraph {
		private final class FilteredImmutableGraphNodeIterator extends NodeIterator {
			private final NodeIterator nodeIterator;
			private final long nextNode;
			private long outdegree;
			private long[][] succ;

			public FilteredImmutableGraphNodeIterator(final NodeIterator nodeIterator) {
				this(nodeIterator, 0, -1, LongBigArrays.EMPTY_BIG_ARRAY);
			}

			public FilteredImmutableGraphNodeIterator(final NodeIterator nodeIterator, final long nextNode, final long outdegree, final long[][] succ) {
				this.nodeIterator = nodeIterator;
				this.nextNode = nextNode;
				this.outdegree = outdegree;
				this.succ = succ;
			}

			@Override
			public long outdegree() {
				if (outdegree == -1) throw new IllegalStateException();
				return outdegree;
			}

			@Override
			public long nextLong() {
				final long currNode = nodeIterator.nextLong();
				final long oldOutdegree = nodeIterator.outdegree();
				final LazyLongIterator oldSucc = nodeIterator.successors();
				succ = ensureCapacity(succ, oldOutdegree, 0);
				outdegree = 0;
				for(long i = 0; i < oldOutdegree; i++) {
					final long s = oldSucc.nextLong();
					if (filter.accept(currNode, s)) set(succ, outdegree++, s);
				}
				return currNode;
			}

			@Override
			public long[][] successorBigArray() {
				if (outdegree == -1) throw new IllegalStateException();
				return succ;
			}

			@Override
			public boolean hasNext() {
				return nodeIterator.hasNext();
			}

			@Override
			public NodeIterator copy(final long upperBound) {
				return new FilteredImmutableGraphNodeIterator(nodeIterator.copy(upperBound), nextNode, outdegree, BigArrays.copy(succ, 0, Math.max(0, outdegree)));
			}
		}

		private final ArcFilter filter;
		private final ImmutableGraph graph;
		private long[][] succ;
		private long currentNode = -1;

		private FilteredImmutableGraph(final ArcFilter filter, final ImmutableGraph graph) {
			this.filter = filter;
			this.graph = graph;
		}

		@Override
		public long numNodes() {
			return graph.numNodes();
		}

		@Override
		public FilteredImmutableGraph copy() {
			return new FilteredImmutableGraph(filter, graph.copy());
		}

		@Override
		public boolean randomAccess() {
			return graph.randomAccess();
		}

		@Override
		public boolean hasCopiableIterators() {
			return graph.hasCopiableIterators();
		}

		@Override
		public LazyLongIterator successors(final long x) {
			return new AbstractLazyLongIterator() {

				private final LazyLongIterator succ = graph.successors(x);

				@Override
				public long nextLong() {
					long t;
					while ((t = succ.nextLong()) != -1) if (filter.accept(x, t)) return t;
					return -1;
				}
			};
		}

		@Override
		public long[][] successorBigArray(final long x) {
			if (currentNode != x) {
				succ = LazyLongIterators.unwrap(successors(x));
				currentNode = x ;
			}
			return succ;
		}

		@Override
		public long outdegree(final long x) {
			if (currentNode != x) {
				succ = successorBigArray(x);
				currentNode = x;
			}
			return length(succ);
		}

		@Override
		public NodeIterator nodeIterator() {
			return new FilteredImmutableGraphNodeIterator(graph.nodeIterator());
		}

		@Override
		public NodeIterator nodeIterator(final long from) {
			return new FilteredImmutableGraphNodeIterator(graph.nodeIterator(from), from, -1, LongBigArrays.EMPTY_BIG_ARRAY);
		}

	}

	/** A class that exposes an arc-labelled immutable graph viewed through a filter. */
	private static final class FilteredArcLabelledImmutableGraph extends ArcLabelledImmutableGraph {
		private final LabelledArcFilter filter;
		private final ArcLabelledImmutableGraph graph;
		private long[][] succ;
		private Label[][] label;
		private long cachedNode = -1;

		private final class FilteredArcLabelledNodeIterator extends ArcLabelledNodeIterator {
			private final ArcLabelledNodeIterator nodeIterator;
			private final long upperBound;
			private long currNode;
			private long outdegree;

			public FilteredArcLabelledNodeIterator(final long upperBound) {
				this(upperBound, graph.nodeIterator(), -1, -1);
			}

			public FilteredArcLabelledNodeIterator(final long upperBound, final ArcLabelledNodeIterator nodeIterator, final long currNode, final long outdegree) {
				this.upperBound = upperBound;
				this.nodeIterator = nodeIterator;
				this.currNode = currNode;
				this.outdegree = outdegree;
			}

			@Override
			public long outdegree() {
				if (currNode == -1) throw new IllegalStateException();
				if (outdegree == -1) {
					long d = 0;
					final LabelledArcIterator successors = successors();
					while(successors.nextLong() != -1) d++;
					outdegree = d;
				}
				return outdegree;
			}

			@Override
			public long nextLong() {
				outdegree = -1;
				return currNode = nodeIterator.nextLong();
			}

			@Override
			public boolean hasNext() {
				return currNode + 1 < upperBound && nodeIterator.hasNext();
			}

			@Override
			public LabelledArcIterator successors() {
				return new FilteredLabelledArcIterator(currNode, nodeIterator.successors());
			}

			@Override
			public ArcLabelledNodeIterator copy(final long upperBound) {
				return new FilteredArcLabelledNodeIterator(upperBound, nodeIterator.copy(upperBound), currNode, outdegree);
			}
		}

		private final class FilteredLabelledArcIterator extends AbstractLazyLongIterator implements LabelledArcIterator {
			private final long x;

			private final LabelledArcIterator successors;

			private FilteredLabelledArcIterator(final long x, final LabelledArcIterator successors) {
				this.x = x;
				this.successors = successors;
			}

			@Override
			public long nextLong() {
				long t;
				while ((t = successors.nextLong()) != -1) if (filter.accept(x, t, successors.label())) return t;
				return -1;
			}

			@Override
			public Label label() {
				return successors.label();
			}
		}

		private FilteredArcLabelledImmutableGraph(final LabelledArcFilter filter, final ArcLabelledImmutableGraph graph) {
			this.filter = filter;
			this.graph = graph;
		}

		@Override
		public long numNodes() {
			return graph.numNodes();
		}

		@Override
		public ArcLabelledImmutableGraph copy() {
			return new FilteredArcLabelledImmutableGraph(filter, graph.copy());
		}

		@Override
		public boolean randomAccess() {
			return graph.randomAccess();
		}

		@Override
		public Label prototype() {
			return graph.prototype();
		}

		private void fillCache(final long x) {
			if (x == cachedNode) return;
			cachedNode = x;
			succ = LazyLongIterators.unwrap(successors(x));
			label = super.labelBigArray(x);
		}

		@Override
		public LabelledArcIterator successors(final long x) {
			return new FilteredLabelledArcIterator(x, graph.successors(x));
		}

		@Override
		public long[][] successorBigArray(final long x) {
			fillCache(x);
			return succ;
		}

		@Override
		public Label[][] labelBigArray(final long x) {
			fillCache(x);
			return label;
		}

		@Override
		public long outdegree(final long x) {
			fillCache(x);
			return length(succ);
		}

		@Override
		public ArcLabelledNodeIterator nodeIterator() {
			return new FilteredArcLabelledNodeIterator(Long.MAX_VALUE);
		}

	}

	/** Returns a graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @param ignored a progress logger, which will be ignored.
	 * @return the filtered graph.
	 */
	public static ImmutableGraph filterArcs(final ImmutableGraph graph, final ArcFilter filter, final ProgressLogger ignored) {
		return filterArcs(graph, filter);
	}

	/** Returns a labelled graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a labelled graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @param ignored a progress logger, which will be ignored.
	 * @return the filtered graph.
	 */
	public static ArcLabelledImmutableGraph filterArcs(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final ProgressLogger ignored) {
		return filterArcs(graph, filter);
	}

	/** Returns a graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @return the filtered graph.
	 */
	public static ImmutableGraph filterArcs(final ImmutableGraph graph, final ArcFilter filter) {
		return new FilteredImmutableGraph(filter, graph);
	}

	/** Returns a labelled graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a labelled graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @return the filtered graph.
	 */
	public static ArcLabelledImmutableGraph filterArcs(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter) {
		return new FilteredArcLabelledImmutableGraph(filter, graph);
	}


	/** Returns a symmetrized graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return the symmetrized graph.
	 * @see #symmetrizeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph symmetrizeOffline(final ImmutableGraph g, final int batchSize) throws IOException {
		return symmetrizeOffline(g, batchSize, null, null);
	}

	/** Returns a symmetrized graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return the symmetrized graph.
	 * @see #symmetrizeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph symmetrizeOffline(final ImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return symmetrizeOffline(g, batchSize, tempDir, null);
	}

	/** Returns a symmetrized graph using an offline transposition.
	 *
	 * <P>The symmetrized graph is the union of a graph and of its transpose. This method will
	 * compute the transpose on the fly using {@link #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)}.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the symmetrized graph.
	 */
	public static ImmutableGraph symmetrizeOffline(final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		return union(g, transposeOffline(g, batchSize, tempDir, pl));
	}

	/**
	 * Returns a simplified (loopless and symmetric) graph using the graph and its transpose.
	 *
	 * @param g the source graph.
	 * @param t the graph <code>g</code> transposed.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the simplified (loopless and symmetric) graph.
	 */
	public static ImmutableGraph simplify(final ImmutableGraph g, final ImmutableGraph t, final ProgressLogger pl) {
		return filterArcs(union(g, t), NO_LOOPS, pl);
	}

	/**
	 * Returns a simplified (loopless and symmetric) graph using the graph and its transpose.
	 *
	 * @param g the source graph.
	 * @param t the graph <code>g</code> transposed.
	 * @return the simplified (loopless and symmetric) graph.
	 */
	public static ImmutableGraph simplify(final ImmutableGraph g, final ImmutableGraph t) {
		return filterArcs(union(g, t), NO_LOOPS, null);
	}

	/**
	 * Returns a simplified (loopless and symmetric) graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @return the simplified (loopless and symmetric) graph.
	 * @see #simplifyOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph simplifyOffline(final ImmutableGraph g, final int batchSize) throws IOException {
		return simplifyOffline(g, batchSize, null, null);
	}

	/**
	 * Returns a simplified (loopless and symmetric) graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return the simplified (loopless and symmetric) graph.
	 * @see #simplifyOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph simplifyOffline(final ImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return simplifyOffline(g, batchSize, tempDir, null);
	}

	/**
	 * Returns a simplified graph(loopless and symmetric) using an offline transposition.
	 *
	 * <P>
	 * The simplified graph is the union of a graph and of its transpose, with the loops removed. This
	 * method will compute the transpose on the fly using
	 * {@link #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)}.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the simplified (loopless and symmetric) graph.
	 */
	public static ImmutableGraph simplifyOffline(final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		return filterArcs(symmetrizeOffline(g, batchSize, tempDir, pl), NO_LOOPS);
	}

	/* Provides a sequential immutable graph by merging batches on the fly. */
	public final static class BatchGraph extends ImmutableSequentialGraph {
		private final class BatchGraphNodeIterator extends NodeIterator {
			/** The buffer size. We can't make it too big&mdash;there's one per batch, per thread. */
			private static final int STD_BUFFER_SIZE = 128 * 1024;
			/** The indirect queue used to merge the batches. */
			private final LongHeapSemiIndirectPriorityQueue queue;
			/** The reference array for {@link #queue}. */
			private final long[] refArray;
			/** The input bit streams over the batches. */
			private final InputBitStream[] batchIbs;
			/** The number of elements in each each {@linkplain #batchIbs batch}. */
			private final int[] inputStreamLength;
			/** The limit for {@link #hasNext()}. */
			private final long hasNextLimit;
			/** The target of the lastly returned arcs */
			private final long[] prevTarget;
			/** The last returned node (-1 if no node has been returned yet). */
			private long last;
			/** The outdegree of the current node (valid if not -1). */
			private long outdegree;
			/** The number of pairs associated with the current node (valid if {@link #last} is not -1). */
			private long numPairs;
			/** The successors of the current node (valid if {@link #last} is not -1);
			 * only the first {@link #outdegree} entries are meaningful. */
			private long[][] successor = LongBigArrays.EMPTY_BIG_ARRAY;
			/** The batches underlying this iterator. */
			private final ObjectArrayList<File> batches;
			/** The number of nodes in the graph. */
			private final long n;

			private BatchGraphNodeIterator(final long n, final ObjectArrayList<File> batches, final long upperBound) throws IOException {
				this(n, batches, upperBound, null, null, null, null, -1, -1, LongBigArrays.EMPTY_BIG_ARRAY);
			}

			private BatchGraphNodeIterator(final long n, final ObjectArrayList<File> batches, final long upperBound, final InputBitStream[] baseIbs, final long[] refArray, final long[] prevTarget, final int[] inputStreamLength, final long last, final long outdegree, final long successor[][]) throws IOException {
				this.n = n;
				this.batches = batches;
				this.hasNextLimit = Math.min(n, upperBound) - 1;
				this.last = last;
				this.outdegree = outdegree;
				this.successor = successor;
				batchIbs = new InputBitStream[batches.size()];

				if (refArray == null) {
					this.refArray = new long[batches.size()];
					this.prevTarget = new long[batches.size()];
					this.inputStreamLength = new int[batches.size()];
					Arrays.fill(this.prevTarget, -1);
					queue = new LongHeapSemiIndirectPriorityQueue(this.refArray);
					// We open all files and load the first element into the reference array.
					for (int i = 0; i < batches.size(); i++) {
						batchIbs[i] = new InputBitStream(batches.get(i), STD_BUFFER_SIZE);
						this.inputStreamLength[i] = batchIbs[i].readDelta();
						this.refArray[i] = batchIbs[i].readLongDelta();
						queue.enqueue(i);
					}
				} else {
					this.refArray = refArray;
					this.prevTarget = prevTarget;
					this.inputStreamLength = inputStreamLength;
					queue = new LongHeapSemiIndirectPriorityQueue(refArray);

					for (int i = 0; i < refArray.length; i++) {
						if (baseIbs[i] != null) {
							batchIbs[i] = new InputBitStream(batches.get(i), STD_BUFFER_SIZE);
							batchIbs[i].position(baseIbs[i].position());
							queue.enqueue(i);
						}
					}
				}
			}

			@Override
			public NodeIterator copy(final long upperBound) {
				try {
					if (last == -1) return new BatchGraphNodeIterator(n, batches, upperBound);
					else return new BatchGraphNodeIterator(n, batches, upperBound, batchIbs, refArray.clone(), prevTarget.clone(), inputStreamLength.clone(), last, outdegree(), BigArrays.copy(successor, 0, outdegree()));
				} catch (final IOException e) {
					throw new RuntimeException(e.getMessage(), e);
				}
			}

			@Override
			public long outdegree() {
				if (last == -1) throw new IllegalStateException();
				if (outdegree == -1) successorBigArray();
				return outdegree;
			}

			@Override
			public boolean hasNext() {
				return last < hasNextLimit;
			}

			@Override
			public long nextLong() {
				if (!hasNext()) throw new NoSuchElementException();
				last++;
				long d = 0;
				outdegree = -1;
				int i;

				try {
					/* We extract elements from the queue as long as their target is equal
					 * to last. If during the process we exhaust a batch, we close it. */

					while(! queue.isEmpty() && refArray[i = queue.first()] == last) {
						successor = grow(successor, d + 1);
						set(successor, d, prevTarget[i] += batchIbs[i].readLongDelta() + 1);
						if (--inputStreamLength[i] == 0) {
							queue.dequeue();
							batchIbs[i].close();
							batchIbs[i] = null;
						}
						else {
							// We read a new source and update the queue.
							final long sourceDelta = batchIbs[i].readLongDelta();
							if (sourceDelta != 0) {
								refArray[i] += sourceDelta;
								prevTarget[i] = -1;
								queue.changed();
							}
						}
						d++;
					}

					numPairs = d;
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}

				return last;
			}

			@Override
			public long[][] successorBigArray() {
				if (last == -1) throw new IllegalStateException();
				if (outdegree == -1) {
					final long numPairs = this.numPairs;
					// Neither quicksort nor heaps are stable, so we reestablish order here.
					LongBigArrays.quickSort(successor, 0, numPairs);
					if (numPairs != 0) {
						long p = 0;
						long pSuccessor = get(successor, p);

						for (long j = 1; j < numPairs; j++) {
							final long s = get(successor, j);
							if (pSuccessor != s) {
								set(successor, ++p, s);
								pSuccessor = s;
							}
						}
						outdegree = p + 1;
					} else outdegree = 0;
				}
				return successor;
			}

			@SuppressWarnings("deprecation")
			@Override
			protected void finalize() throws Throwable {
				try {
					for(final InputBitStream ibs: batchIbs) if (ibs != null) ibs.close();
				}
				finally {
					super.finalize();
				}
			}
		}

		private final ObjectArrayList<File> batches;
		private final long n;
		private final long numArcs;

		public BatchGraph(final long n, final long m, final ObjectArrayList<File> batches) {
			this.batches = batches;
			this.n = n;
			this.numArcs = m;
		}

		@Override
		public long numNodes() { return n; }
		@Override
		public long numArcs() {
			if (numArcs == -1) throw new UnsupportedOperationException();
			return numArcs;
		}

		@Override
		public boolean hasCopiableIterators() {
			return true;
		}

		@Override
		public BatchGraph copy() {
			return this;
		}

		@Override
		public NodeIterator nodeIterator() {
			try {
				return new BatchGraphNodeIterator(n, batches, n);
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		protected void finalize() throws Throwable {
			try {
				for (final File f : batches) f.delete();
			}
			finally {
				super.finalize();
			}
		}
	}

	/** Sorts the given source and target arrays w.r.t. the target and stores them in a temporary file.
	 *
	 * @param n the index of the last element to be sorted (exclusive).
	 * @param source the source array.
	 * @param target the target array.
	 * @param tempDir a temporary directory where to store the sorted arrays, or <code>null</code>
	 * @param batches a list of files to which the batch file will be added.
	 * @return the number of pairs in the batch (might be less than <code>n</code> because duplicates are eliminated).
	 */

	public static int processBatch(final int n, final long[] source, final long[] target, final File tempDir, final List<File> batches) throws IOException {

		LongArrays.parallelQuickSort(source, target, 0, n);

		final File batchFile = File.createTempFile("batch", ".bitstream", tempDir);
		batchFile.deleteOnExit();
		batches.add(batchFile);
		final OutputBitStream batch = new OutputBitStream(batchFile);
		int u = 0;
		if (n != 0) {
			// Compute unique pairs
			u = 1;
			for(int i = n - 1; i-- != 0;) if (source[i] != source[i + 1] || target[i] != target[i + 1]) u++;
			batch.writeDelta(u);
			long prevSource = source[0];
			batch.writeLongDelta(prevSource);
			batch.writeLongDelta(target[0]);

			for(int i = 1; i < n; i++) {
				if (source[i] != prevSource) {
					batch.writeLongDelta(source[i] - prevSource);
					batch.writeLongDelta(target[i]);
					prevSource = source[i];
				}
				else if (target[i] != target[i - 1]) {
					// We don't write duplicate pairs
					batch.writeDelta(0);
					assert target[i] > target[i - 1] : target[i] + "<=" + target[i - 1];
					batch.writeLongDelta(target[i] - target[i - 1] - 1);
				}
			}
		}
		else batch.writeDelta(0);

		batch.close();
		return u;
	}

	/** Sorts the given source and target arrays w.r.t. the target and stores them in two temporary files.
	 *  An additional positionable input bit stream is provided that contains labels, starting at given positions.
	 *  Labels are also written onto the appropriate file.
	 *
	 * @param n the index of the last element to be sorted (exclusive).
	 * @param source the source array.
	 * @param target the target array.
	 * @param start the array containing the bit position (within the given input stream) where the label of the arc starts.
	 * @param labelBitStream the positionable bit stream containing the labels.
	 * @param tempDir a temporary directory where to store the sorted arrays.
	 * @param batches a list of files to which the batch file will be added.
	 * @param labelBatches a list of files to which the label batch file will be added.
	 */

	private static void processTransposeBatch(final int n, final long[] source, final long[] target, final long[] start,
			final InputBitStream labelBitStream, final File tempDir, final List<File> batches, final List<File> labelBatches,
			final Label prototype) throws IOException {
		it.unimi.dsi.fastutil.Arrays.parallelQuickSort(0, n, (x, y) -> {
			final int t = Long.compare(source[x], source[y]);
			if (t != 0) return t;
			return Long.compare(target[x], target[y]);
		}, (x, y) -> {
			long t = source[x];
			source[x] = source[y];
			source[y] = t;
			t = target[x];
			target[x] = target[y];
			target[y] = t;
			final long u = start[x];
			start[x] = start[y];
			start[y] = u;
		});

		final File batchFile = File.createTempFile("batch", ".bitstream", tempDir);
		batchFile.deleteOnExit();
		batches.add(batchFile);
		final OutputBitStream batch = new OutputBitStream(batchFile);

		if (n != 0) {
			// Compute unique pairs
			batch.writeDelta(n);
			long prevSource = source[0];
			batch.writeLongDelta(prevSource);
			batch.writeLongDelta(target[0]);

			for(int i = 1; i < n; i++) {
				if (source[i] != prevSource) {
					batch.writeLongDelta(source[i] - prevSource);
					batch.writeLongDelta(target[i]);
					prevSource = source[i];
				}
				else if (target[i] != target[i - 1]) {
					// We don't write duplicate pairs
					batch.writeDelta(0);
					batch.writeLongDelta(target[i] - target[i - 1] - 1);
				}
			}
		}
		else batch.writeDelta(0);

		batch.close();

		final File labelFile = File.createTempFile("label-", ".bits", tempDir);
		labelFile.deleteOnExit();
		labelBatches.add(labelFile);
		final OutputBitStream labelObs = new OutputBitStream(labelFile);
		for (int i = 0; i < n; i++) {
			labelBitStream.position(start[i]);
			prototype.fromBitStream(labelBitStream, source[i]);
			prototype.toBitStream(labelObs, target[i]);
		}
		labelObs.close();
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */

	public static ImmutableSequentialGraph transposeOffline(final ImmutableGraph g, final int batchSize) throws IOException {
		return transposeOffline(g, batchSize, null);
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */

	public static ImmutableSequentialGraph transposeOffline(final ImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return transposeOffline(g, batchSize, tempDir, null);
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * <p>This method creates a number of sorted batches on disk containing arcs
	 * represented by a pair of gap-compressed long integers ordered by target
	 * and returns an {@link ImmutableGraph}
	 * that can be accessed only using a {@link ImmutableGraph#nodeIterator() node iterator}. The node iterator
	 * merges on the fly the batches, providing a transposed graph. The files are marked with
	 * {@link File#deleteOnExit()}, so they should disappear when the JVM exits. An additional safety-net
	 * finaliser tries to delete the batches, too.
	 *
	 * <p>Note that each {@link NodeIterator} returned by the transpose requires opening all batches at the same time.
	 * The batches are closed when they are exhausted, so a complete scan of the graph closes them all. In any case,
	 * another safety-net finaliser closes all files when the iterator is collected.
	 *
	 * <P>This method can process {@linkplain ImmutableGraph#loadOffline(CharSequence) offline graphs}.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 */

	public static ImmutableSequentialGraph transposeOffline(final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		final long[] source = new long[batchSize] , target = new long[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		final long n = g.numNodes();

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start("Creating sorted batches...");
		}

		final NodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <source,target> and dumping them on disk.
		LazyLongIterator succ;
		long m = 0; // Number of arcs, computed on the fly.
		int j = 0;
		for (long i = n; i-- != 0;) {
			final long currNode = nodeIterator.nextLong();
			final long d = nodeIterator.outdegree();
			succ = nodeIterator.successors();
			m += d;

			for(long k = 0; k < d; k++) {
				target[j] = currNode;
				source[j++] = succ.nextLong();

				if (j == batchSize) {
					processBatch(batchSize, source, target, tempDir, batches);
					j = 0;
				}
			}


			if (pl != null) pl.lightUpdate();
		}

		if (j != 0) processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			logBatches(batches, m, pl);
		}

		return new BatchGraph(n, m, batches);
	}

	protected static void logBatches(final ObjectArrayList<File> batches, final long pairs, final ProgressLogger pl) {
		long length = 0;
		for(final File f : batches) length += f.length();
		pl.logger().info("Created " + batches.size() + " batches using " + Util.format((double)Byte.SIZE * length / pairs) + " bits/arc.");
	}

	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via a big array.
	 *
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 * @see #mapOffline(ImmutableGraph, long[][], int, File, ProgressLogger)
	 */
	public static ImmutableSequentialGraph mapOffline(final ImmutableGraph g, final long map[][], final int batchSize) throws IOException {
		return mapOffline(g, map, batchSize, null);
	}

	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via a big array.
	 *
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 * @see #mapOffline(ImmutableGraph, long[][], int, File, ProgressLogger)
	 */
	public static ImmutableSequentialGraph mapOffline(final ImmutableGraph g, final long map[][], final int batchSize, final File tempDir) throws IOException {
		return mapOffline(g, map, batchSize, tempDir, null);
	}

	/** Remaps the the graph nodes through a partial function specified via
	 * a big array, using an offline method.
	 *
	 * <p>More specifically, <code>LongBigArrays.length(map)=g.numNodes()</code>,
	 * and <code>LongBigArrays.get(map, i)</code> is the new name of node <code>i</code>, or -1 if the node
	 * should not be mapped. If some
	 * index appearing in <code>map</code> is larger than or equal to the
	 * number of nodes of <code>g</code>, the resulting graph is enlarged correspondingly.
	 *
	 * <P>Arcs are mapped in the obvious way; in other words, there is
	 * an arc from <code>LongBigArrays.get(map, i)</code> to <code>LongBigArrays.get(map, j)</code> (both nonnegative)
	 * in the transformed
	 * graph iff there was an arc from <code>i</code> to <code>j</code>
	 * in the original graph.
	 *
	 *  <P>Note that if <code>map</code> is bijective, the returned graph
	 *  is simply a permutation of the original graph.
	 *  Otherwise, the returned graph is obtained by deleting nodes mapped
	 *  to -1, quotienting nodes w.r.t. the equivalence relation induced by the fibres of <code>map</code>
	 *  and renumbering the result, always according to <code>map</code>.
	 *
	 * See {@link #transposeOffline(ImmutableGraph, int, File, ProgressLogger)} for
	 * implementation and performance-related details.
	 *
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 */
	public static ImmutableSequentialGraph mapOffline(final ImmutableGraph g, final long map[][], final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {

		int j;
		long d, mappedCurrNode;
		final long[] source = new long[batchSize] , target = new long[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		long max = -1;
		for(int i = map.length; i-- != 0;) {
			final long[] t = map[i];
			for(int k = t.length; k-- != 0;) max = Math.max(max, t[k]);
		}

		final long mapLength = length(map);

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = mapLength;
			pl.start("Creating sorted batches...");
		}

		final NodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <map[source],map[target]> (if we have to) and dumping them on disk.
		LazyLongIterator succ;
		j = 0;
		long pairs = 0; // Number of pairs
		for(long i = g.numNodes(); i-- != 0;) {
			mappedCurrNode = get(map, nodeIterator.nextLong());
			if (mappedCurrNode != -1) {
				d = nodeIterator.outdegree();
				succ = nodeIterator.successors();

				for(long k = 0; k < d; k++) {
					final long s = succ.nextLong();
					if (get(map, s) != -1) {
						source[j] = mappedCurrNode;
						target[j++] = get(map, s);

						if (j == batchSize) {
							pairs += processBatch(batchSize, source, target, tempDir, batches);
							j = 0;
						}
					}
				}
			}

			if (pl != null) pl.lightUpdate();
		}

		// At this point the number of nodes is always known (a traversal has been completed).
		if (g.numNodes() != mapLength) throw new IllegalArgumentException("Mismatch between number of nodes (" + g.numNodes() + ") and map length (" + mapLength + ")");

		if (j != 0) pairs += processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			logBatches(batches, pairs, pl);
		}

		return new BatchGraph(max + 1, -1, batches);
	}



	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)
	 */
	public static ArcLabelledImmutableGraph transposeOffline(final ArcLabelledImmutableGraph g, final int batchSize) throws IOException {
		return transposeOffline(g, batchSize, null);
	}

	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)
	 */
	public static ArcLabelledImmutableGraph transposeOffline(final ArcLabelledImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return transposeOffline(g, batchSize, tempDir, null);
	}


	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * <p>This method creates a number of sorted batches on disk containing arcs
	 * represented by a pair of long integers in {@link java.io.DataInput} format ordered by target
	 * and returns an {@link ImmutableGraph}
	 * that can be accessed only using a {@link ImmutableGraph#nodeIterator() node iterator}. The node iterator
	 * merges on the fly the batches, providing a transposed graph. The files are marked with
	 * {@link File#deleteOnExit()}, so they should disappear when the JVM exits. An additional safety-net
	 * finaliser tries to delete the batches, too. As far as labels are concerned, they are temporarily stored in
	 * an in-memory bit stream, that is permuted when it is stored on the disk
	 *
	 * <p>Note that each {@link NodeIterator} returned by the transpose requires opening all batches at the same time.
	 * The batches are closed when they are exhausted, so a complete scan of the graph closes them all. In any case,
	 * another safety-net finaliser closes all files when the iterator is collected.
	 *
	 * <P>This method can process {@linkplain ArcLabelledImmutableGraph#loadOffline(CharSequence) offline graphs}. Note that
	 * no method to transpose on-line arc-labelled graph is provided currently.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 */

	public static ArcLabelledImmutableGraph transposeOffline(final ArcLabelledImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {

		int j;
		long d, currNode;
		final long[] source = new long[batchSize] , target = new long[batchSize];
		final long[] start = new long[batchSize];
		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream(fbos);
		final ObjectArrayList<File> batches = new ObjectArrayList<>(), labelBatches = new ObjectArrayList<>();
		final Label prototype = g.prototype().copy();

		final long n = g.numNodes();

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start("Creating sorted batches...");
		}

		final ArcLabelledNodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <source,target> and dumping them on disk.
		LabelledArcIterator succ;
		Label[][] label = null;
		long m = 0; // Number of arcs, computed on the fly.
		j = 0;
		for(long i = n; i-- != 0;) {
			currNode = nodeIterator.nextLong();
			d = nodeIterator.outdegree();
			succ = nodeIterator.successors();
			label = nodeIterator.labelBigArray();
			m += d;

			for(long k = 0; k < d; k++) {
				source[j] = succ.nextLong();
				target[j] = currNode;
				start[j] = obs.writtenBits();
				get(label, k).toBitStream(obs, currNode);
				j++;

				if (j == batchSize) {
					obs.flush();
					processTransposeBatch(batchSize, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype);
					fbos = new FastByteArrayOutputStream();
					obs = new OutputBitStream(fbos); //ALERT here we should re-use
					j = 0;
				}
			}

			if (pl != null) pl.lightUpdate();
		}

		if (j != 0) {
			obs.flush();
			processTransposeBatch(j, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype);
		}

		if (pl != null) {
			pl.done();
			logBatches(batches, m, pl);
		}

		final long numArcs = m;

		// Now we return an immutable graph whose nodeIterator() merges the batches on the fly.
		return new ArcLabelledImmutableSequentialGraph() {
			@Override
			public long numNodes() { return n; }
			@Override
			public long numArcs() { return numArcs; }
			@Override
			public boolean hasCopiableIterators() {
				return true;
			}

			class InternalArcLabelledNodeIterator extends ArcLabelledNodeIterator {
				/** The buffer size. We can't make it too big&mdash;there's two per batch, per thread. */
				private static final int STD_BUFFER_SIZE = 64 * 1024;
				private final long[] refArray;
				private final InputBitStream[] batchIbs;
				private final int[] inputStreamLength;
				private final InputBitStream[] labelInputBitStream;
				private final long[] prevTarget;

				// The indirect queue used to merge the batches.
				private final LongHeapSemiIndirectPriorityQueue queue;
				/** The limit for {@link #hasNext()}. */
				private final long hasNextLimit;

				/** The last returned node (-1 if no node has been returned yet). */
				private long last;
				/** The outdegree of the current node (valid if {@link #last} is not -1). */
				private long outdegree;
				/**
				 * The successors of the current node (valid if {@link #last} is not -1); only the first
				 * {@link #outdegree} entries are meaningful.
				 */
				private long[][] successor;
				/**
				 * The labels of the arcs going out of the current node (valid if {@link #last} is not -1); only the
				 * first {@link #outdegree} entries are meaningful.
				 */
				private Label[][] label;

				public InternalArcLabelledNodeIterator(final long upperBound) throws IOException {
					this(upperBound, null, null, null, null, null, -1, 0, LongBigArrays.EMPTY_BIG_ARRAY, Label.EMPTY_LABEL_BIG_ARRAY);
				}

				public InternalArcLabelledNodeIterator(final long upperBound, final InputBitStream[] baseIbs, final InputBitStream[] baseLabelInputBitStream, final long[] refArray, final long[] prevTarget, final int[] inputStreamLength, final long last, final long outdegree, final long successor[][], final Label[][] label) throws IOException {
					this.hasNextLimit = Math.min(n, upperBound) - 1;
					this.last = last;
					this.outdegree = outdegree;
					this.successor = successor;
					this.label = label;
					batchIbs = new InputBitStream[batches.size()];
					labelInputBitStream = new InputBitStream[batches.size()];

					if (refArray == null) {
						this.refArray = new long[batches.size()];
						this.prevTarget = new long[batches.size()];
						this.inputStreamLength = new int[batches.size()];
						Arrays.fill(this.prevTarget, -1);
						queue = new LongHeapSemiIndirectPriorityQueue(this.refArray);
						// We open all files and load the first element into the reference array.
						for (int i = 0; i < batches.size(); i++) {
							batchIbs[i] = new InputBitStream(batches.get(i), STD_BUFFER_SIZE);
							labelInputBitStream[i] = new InputBitStream(labelBatches.get(i), STD_BUFFER_SIZE);
							this.inputStreamLength[i] = batchIbs[i].readDelta();
							this.refArray[i] = batchIbs[i].readDelta();
							queue.enqueue(i);
						}
					} else {
						this.refArray = refArray;
						this.prevTarget = prevTarget;
						this.inputStreamLength = inputStreamLength;
						queue = new LongHeapSemiIndirectPriorityQueue(refArray);

						for (int i = 0; i < refArray.length; i++) {
							if (baseIbs[i] != null) {
								batchIbs[i] = new InputBitStream(batches.get(i), STD_BUFFER_SIZE);
								batchIbs[i].position(baseIbs[i].position());
								labelInputBitStream[i] = new InputBitStream(labelBatches.get(i), STD_BUFFER_SIZE);
								labelInputBitStream[i].position(baseLabelInputBitStream[i].position());
								queue.enqueue(i);
							}
						}
					}
				}

				@Override
				public long outdegree() {
					if (last == -1) throw new IllegalStateException();
					return outdegree;
				}

				@Override
				public boolean hasNext() {
					return last < hasNextLimit;
				}

				@Override
				public long nextLong() {
					last++;
					long d = 0;
					int i;

					try {
						/*
						 * We extract elements from the queue as long as their target is equal to last. If during the
						 * process we exhaust a batch, we close it.
						 */

						while (!queue.isEmpty() && refArray[i = queue.first()] == last) {
							successor = grow(successor, d + 1);
							set(successor, d, prevTarget[i] += batchIbs[i].readLongDelta() + 1);
							label = grow(label, d + 1);
							final Label l = prototype.copy();
							set(label, d, l);
							l.fromBitStream(labelInputBitStream[i], last);

							if (--inputStreamLength[i] == 0) {
								queue.dequeue();
								batchIbs[i].close();
								labelInputBitStream[i].close();
								batchIbs[i] = null;
								labelInputBitStream[i] = null;
							} else {
								// We read a new source and update the queue.
								final long sourceDelta = batchIbs[i].readLongDelta();
								if (sourceDelta != 0) {
									refArray[i] += sourceDelta;
									prevTarget[i] = -1;
									queue.changed();
								}
							}
							d++;
						}
						// Neither quicksort nor heaps are stable, so we reestablish order here.
						it.unimi.dsi.fastutil.BigArrays.quickSort(0, d, (x, y) -> {
							return Long.compare(get(successor, x), get(successor, y));
						}, (x, y) -> {
							final long t = get(successor, x);
							set(successor, x, get(successor, y));
							set(successor, y, t);
							final Label l = get(label, x);
							set(label, x, get(label, y));
							set(label, y, l);
						});
					} catch (final IOException e) {
						throw new RuntimeException(e);
					}

					outdegree = d;
					return last;
				}

				@SuppressWarnings("deprecation")
				@Override
				protected void finalize() throws Throwable {
					try {
						for (final InputBitStream ibs : batchIbs) if (ibs != null) ibs.close();
						for (final InputBitStream ibs : labelInputBitStream) if (ibs != null) ibs.close();
					} finally {
						super.finalize();
					}
				}

				@Override
				public LabelledArcIterator successors() {
					if (last == -1) throw new IllegalStateException();
					return new LabelledArcIterator() {
						long last = -1;

						@Override
						public Label label() {
							return get(label, last);
						}

						@Override
						public long nextLong() {
							if (last + 1 == outdegree) return -1;
							return get(successor, ++last);
						}

						@Override
						public long skip(final long k) {
							final long toSkip = Math.min(k, outdegree - last - 1);
							last += toSkip;
							return toSkip;
						}
					};
				}

				@Override
				public ArcLabelledNodeIterator copy(final long upperBound) {
					try {
						if (last == -1) return new InternalArcLabelledNodeIterator(upperBound);
						else return new InternalArcLabelledNodeIterator(upperBound, batchIbs, labelInputBitStream,
								refArray.clone(), prevTarget.clone(), inputStreamLength.clone(), last, outdegree, BigArrays.copy(successor, 0, outdegree), BigArrays.copy(label, 0, outdegree));
					}
					catch (final IOException e) {
						throw new RuntimeException(e);
					}
				}
			}

			@Override
			public ArcLabelledNodeIterator nodeIterator() {
				try {
					return new InternalArcLabelledNodeIterator(Long.MAX_VALUE);
				}
				catch (final IOException e) {
					throw new RuntimeException(e);
				}
			}


			@SuppressWarnings("deprecation")
			@Override
			protected void finalize() throws Throwable {
				try {
					for(final File f : batches) f.delete();
					for(final File f : labelBatches) f.delete();
				}
				finally {
					super.finalize();
				}
			}
			@Override
			public Label prototype() {
				return prototype;
			}

		};
	}


	/** Returns the union of two arc-labelled immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param labelMergeStrategy the strategy used to merge labels when the same arc
	 *  is present in both graphs; if <code>null</code>, {@link Labels#KEEP_FIRST_MERGE_STRATEGY}
	 *  is used.
	 * @return the union of the two graphs.
	 */
	public static ArcLabelledImmutableGraph union(final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelMergeStrategy labelMergeStrategy) {
		return new UnionArcLabelledImmutableGraph(g0, g1, labelMergeStrategy == null? Labels.KEEP_FIRST_MERGE_STRATEGY : labelMergeStrategy);
	}

	/** Returns the union of two immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @return the union of the two graphs.
	 */
	public static ImmutableGraph union(final ImmutableGraph g0, final ImmutableGraph g1) {
		return g0 instanceof ArcLabelledImmutableGraph && g1 instanceof ArcLabelledImmutableGraph
			? union((ArcLabelledImmutableGraph)g0, (ArcLabelledImmutableGraph)g1, (LabelMergeStrategy)null)
					: new UnionImmutableGraph(g0, g1);
	}


	private static final class ComposedGraph extends ImmutableSequentialGraph {
		private final class ComposedGraphNodeIterator extends NodeIterator {
			private final NodeIterator it0;
			private final long upperBound;
			private long[][] succ;
			private final LongOpenHashSet successors;
			private long outdegree; // -1 means that the cache is empty
			private long nextNode;

			public ComposedGraphNodeIterator(final long upperBound) {
				this(upperBound, g0.nodeIterator(), LongBigArrays.EMPTY_BIG_ARRAY, new LongOpenHashSet(Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR), -1, 0);
			}

			public ComposedGraphNodeIterator(final long upperBound, final NodeIterator it, final long[][] succ, final LongOpenHashSet successors, final long outdegree, final long nextNode) {
				this.it0 = it;
				this.upperBound = upperBound;
				this.succ = succ;
				this.successors = successors;
				this.outdegree = outdegree;
				this.nextNode = nextNode;
			}

			@Override
			public long nextLong() {
				outdegree = -1;
				nextNode++;
				return it0.nextLong();
			}

			@Override
			public boolean hasNext() {
				return nextNode < upperBound && it0.hasNext();
			}

			@Override
			public long outdegree() {
				if (outdegree < 0) successorBigArray();
				return outdegree;
			}

			@Override
			public long[][] successorBigArray() {
				if (outdegree < 0) {
					final long d = it0.outdegree();
					final long[][] s = it0.successorBigArray();
					successors.clear();
					for (long i = 0; i < d; i++) {
						final LazyLongIterator s1 = g1.successors(get(s, i));
						long x;
						while ((x = s1.nextLong()) >= 0) successors.add(x);
					}
					outdegree = successors.size();
					succ = ensureCapacity(succ, outdegree, 0);
					succ = LongBigArrays.newBigArray(outdegree);
					final LongIterator iterator = successors.iterator();
					for (long i = 0; i < outdegree; i++) set(succ, i, iterator.nextLong());
					LongBigArrays.quickSort(succ, 0, outdegree);
				}
				return succ;
			}

			@Override
			public NodeIterator copy(final long upperBound) {
				return new ComposedGraphNodeIterator(upperBound, it0.copy(Long.MAX_VALUE), BigArrays.copy(succ, 0, outdegree), new LongOpenHashSet(successors), outdegree, nextNode);
			}
		}

		private final ImmutableGraph g0;

		private final ImmutableGraph g1;

		private ComposedGraph(final ImmutableGraph g0, final ImmutableGraph g1) {
			this.g0 = g0;
			this.g1 = g1;
		}

		@Override
		public long numNodes() {
			return Math.max(g0.numNodes(), g1.numNodes());
		}

		@Override
		public ImmutableSequentialGraph copy() {
			// Note that only the second graph needs duplication.
			return new ComposedGraph(g0, g1.copy());
		}

		@Override
		public boolean hasCopiableIterators() {
			return true;
		}

		@Override
		public NodeIterator nodeIterator() {
			return new ComposedGraphNodeIterator(Long.MAX_VALUE);
		}
	}

	/** Returns the composition (a.k.a. matrix product) of two immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @return the composition of the two graphs.
	 */
	public static ImmutableGraph compose(final ImmutableGraph g0, final ImmutableGraph g1) {
		return new ComposedGraph(g0, g1);
	}


	/**
	 * Returns the composition (a.k.a. matrix product) of two arc-labelled immutable graphs.
	 *
	 * <P>
	 * The two arguments may differ in the number of nodes, in which case the resulting graph will be
	 * large as the larger graph.
	 *
	 * @implSpec This implementation <strong>requires</strong> outdegrees smaller than 2<sup>32</sup>.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param strategy a label semiring.
	 * @return the composition of the two graphs.
	 */
	public static ArcLabelledImmutableGraph compose(final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelSemiring strategy) {
		if (g0.prototype().getClass() != g1.prototype().getClass()) throw new IllegalArgumentException("The two graphs have different label classes (" + g0.prototype().getClass().getSimpleName() + ", " +g1.prototype().getClass().getSimpleName() + ")");

		return new ArcLabelledImmutableSequentialGraph() {

			class InternalArcLabelledNodeIterator extends ArcLabelledNodeIterator {
				private final long upperBound;
				private long nextNode;
				private long[] succ = LongArrays.EMPTY_ARRAY;
				private Label[] label = Label.EMPTY_LABEL_ARRAY;
				private int maxOutDegree;
				private int smallCount;
				private Long2ObjectOpenHashMap<Label> successors = new Long2ObjectOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR);
				private int outdegree = -1; // -1 means that the cache is empty
				private final ArcLabelledNodeIterator it0;

				public InternalArcLabelledNodeIterator(final long upperBond) {
					successors.defaultReturnValue(strategy.zero());
					it0 = g0.nodeIterator();
					this.upperBound = upperBond;
				}


				@Override
				public long nextLong() {
					outdegree = -1;
					nextNode++;
					return it0.nextLong();
				}

				@Override
				public boolean hasNext() {
					return nextNode < upperBound && it0.hasNext();
				}

				@Override
				public long outdegree() {
					if (outdegree < 0) successorBigArray();
					return outdegree;
				}

				private void ensureCache() {
					if (outdegree < 0) {
						final long longD = it0.outdegree(); // get the number of successors of currNode
						if (longD > Integer.MAX_VALUE) throw new IllegalArgumentException("This big implementation requires outdegrees to be less than 2^31");
						final int d = (int)longD;
						final LabelledArcIterator s = it0.successors();
						if (successors.size() < maxOutDegree / 2 && smallCount++ > 100) {
							smallCount = 0;
							maxOutDegree = 0;
							successors = new Long2ObjectOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR);
							successors.defaultReturnValue(strategy.zero());
						} else successors.clear();

						for (long i = 0; i < d; i++) {
							final LabelledArcIterator s1 = g1.successors(s.nextLong());
							long x;
							while ((x = s1.nextLong()) >= 0) successors.put(x, strategy.add(strategy.multiply(s.label(), s1.label()), successors.get(x)));
						}
						outdegree = successors.size();
						succ = LongArrays.ensureCapacity(succ, outdegree, 0);
						label = ObjectArrays.ensureCapacity(label, outdegree, 0);
						successors.keySet().toArray(succ);
						LongArrays.quickSort(succ, 0, outdegree);
						for (int i = outdegree; i-- != 0;) label[i] = successors.get(succ[i]);
						if (outdegree > maxOutDegree) maxOutDegree = outdegree;
					}
				}

				@Override
				public long[][] successorBigArray() {
					ensureCache();
					return BigArrays.wrap(succ);
				}

				@Override
				public Label[][] labelBigArray() {
					ensureCache();
					return BigArrays.wrap(label);
				}

				@Override
				public LabelledArcIterator successors() {
					ensureCache();
					return new LabelledArcIterator() {
						int i = -1;

						@Override
						public Label label() {
							return label[i];
						}

						@Override
						public long nextLong() {
							return i < outdegree - 1 ? succ[++i] : -1;
						}

						@Override
						public long skip(final long n) {
							final int incr = (int)Math.min(n, outdegree - i - 1);
							i += incr;
							return incr;
						}
					};
				}
			}
			@Override
			public Label prototype() {
				return g0.prototype();
			}

			@Override
			public long numNodes() {
				return Math.max(g0.numNodes(), g1.numNodes());
			}

			@Override
			public boolean hasCopiableIterators() {
				return g0.hasCopiableIterators() && g1.hasCopiableIterators();
			}

			@Override
			public ArcLabelledNodeIterator nodeIterator() {
				return new InternalArcLabelledNodeIterator(Long.MAX_VALUE);
			}
		};
	}


	/** Returns a permutation that would make the given graph adjacency lists in Gray-code order.
	 *
	 * <P>Gray codes list all sequences of <var>n</var> zeros and ones in such a way that
	 * adjacent lists differ by exactly one bit. If we assign to each row of the adjacency matrix of
	 * a graph its index as a Gray code, we obtain a permutation that will make similar lines
	 * nearer.
	 *
	 * <P>Note that since a graph permutation permutes <em>both</em> rows and columns, this transformation is
	 * not idempotent: the Gray-code permutation produced from a matrix that has been Gray-code sorted will
	 * <em>not</em> be, in general, the identity.
	 *
	 * <P>The important feature of Gray-code ordering is that it is completely endogenous (e.g., determined
	 * by the graph itself), contrarily to, say, lexicographic URL ordering (which relies on the knowledge
	 * of the URL associated to each node).
	 *
	 * @param g an immutable graph.
	 * @return the permutation that would order the graph adjacency lists by Gray order
	 * (you can just pass it to {@link #mapOffline(ImmutableGraph, long[][], int, File, ProgressLogger)}).
	 */
	public static long[][] grayCodePermutation(final ImmutableGraph g) {
		final long n = g.numNodes();
		final long[][] perm = LongBigArrays.newBigArray(n);
		long i = n;
		while (i-- != 0) set(perm, i, i);

		final LongComparator grayComparator =  (x, y) -> {
			final LazyLongIterator i1 = g.successors(x), j = g.successors(y);
			long a;
			long b;

			/* This code duplicates eagerly of the behaviour of the lazy comparator
			   below. It is here for documentation and debugging purposes.

			byte[] g1 = new byte[g.numNodes()], g2 = new byte[g.numNodes()];
			while(i.hasNext()) g1[g.numNodes() - 1 - i.nextInt()] = 1;
			while(j.hasNext()) g2[g.numNodes() - 1 - j.nextInt()] = 1;
			for(int k = g.numNodes() - 2; k >= 0; k--) {
				g1[k] ^= g1[k + 1];
				g2[k] ^= g2[k + 1];
			}
			for(int k = g.numNodes() - 1; k >= 0; k--) if (g1[k] != g2[k]) return g1[k] - g2[k];
			return 0;
			*/

			boolean parity = false; // Keeps track of the parity of number of arcs before the current ones.
			for(;;) {
				a = i1.nextLong();
				b = j.nextLong();
				if (a == -1 && b == -1) return 0;
				if (a == -1) return parity ? 1 : -1;
				if (b == -1) return parity ? -1 : 1;
				if (a != b) return parity ^ (a < b) ? 1 : -1;
				parity = ! parity;
			}
		};

		LongBigArrays.quickSort(perm, 0, n, grayComparator);

		return Util.invertPermutationInPlace(perm);
	}

	/** Returns a random permutation for a given graph.
	 *
	 * @param g an immutable graph.
	 * @param seed for {@link XoRoShiRo128PlusRandom}.
	 * @return a random permutation for the given graph
	 */
	public static long[][] randomPermutation(final ImmutableGraph g, final long seed) {
		return LongBigArrays.shuffle(Util.identity(g.numNodes()), new XoRoShiRo128PlusRandom(seed));
	}



	/** Returns a permutation that would make the given graph adjacency lists in lexicographical order.
	 *
	 * <P>Note that since a graph permutation permutes <em>both</em> rows and columns, this transformation is
	 * not idempotent: the lexicographical permutation produced from a matrix that has been
	 * lexicographically sorted will
	 * <em>not</em> be, in general, the identity.
	 *
	 * <P>The important feature of lexicographical ordering is that it is completely endogenous (e.g., determined
	 * by the graph itself), contrarily to, say, lexicographic URL ordering (which relies on the knowledge
	 * of the URL associated to each node).
	 *
	 * <p><strong>Warning</strong>: rows are numbered from zero <em>from the left</em>. This means,
	 * for instance, that nodes with an arc towards node zero are lexicographically smaller
	 * than nodes without it.
	 *
	 * @param g an immutable graph.
	 * @return the permutation that would order the graph adjacency lists by lexicographical order
	 * (you can just pass it to {@link #mapOffline(ImmutableGraph, long[][], int)}).
	 */
	public static long[][] lexicographicalPermutation(final ImmutableGraph g) {
		final long n = g.numNodes();
		final long[][] perm = Util.identity(n);

		final LongComparator lexicographicalComparator =  (x, y) -> {
			final LazyLongIterator i = g.successors(x), j = g.successors(y);
			long a;
			long b;
			for(;;) {
				a = i.nextLong();
				b = j.nextLong();
				if (a == -1 && b == -1) return 0;
				if (a == -1) return -1;
				if (b == -1) return 1;
				if (a != b) {
					final long t = b - a;
					return t == 0 ? 0 : t < 0 ? -1 : 1;
				}
			}
		};

		LongBigArrays.quickSort(perm, 0, n, lexicographicalComparator);

		return Util.invertPermutationInPlace(perm);
	}



	/** Ensures that the arguments are exactly <code>n</code>, if <code>n</code> is nonnegative, or
	 * at least -<code>n</code>, otherwise.
	 */

	private static boolean ensureNumArgs(final String param[], final int n) {
		if (n >= 0 && param.length != n || n < 0 && param.length < -n) {
			System.err.println("Wrong number (" + param.length + ") of arguments.");
			return false;
		}
		return true;
	}

	/**
	 * Loads a graph with given data and returns it.
	 *
	 * @param graphClass the class of the graph to be loaded.
	 * @param baseName the graph basename.
	 * @param offline whether the graph is to be loaded in an offline fashion.
	 * @param pl a progress logger.
	 * @return the loaded graph.
	 */
	private static ImmutableGraph load(final Class<?> graphClass, final String baseName, final boolean offline, final ProgressLogger pl) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException {
		ImmutableGraph graph = null;

		if (graphClass != null) {
			if (offline) graph = (ImmutableGraph)graphClass.getMethod("loadOffline", CharSequence.class, ProgressLogger.class).invoke(null, baseName, pl);
			else graph = (ImmutableGraph)graphClass.getMethod("load", CharSequence.class).invoke(null, baseName);
		}
		else graph = offline ? ImmutableGraph.loadOffline(baseName) : ImmutableGraph.load(baseName, pl);

		return graph;
	}



	public static void main(final String args[]) throws IOException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, JSAPException {
		Class<?> sourceGraphClass = null, destGraphClass = BVGraph.class;

		final Field[] field = Transform.class.getDeclaredFields();
		final List<String> filterList = new ArrayList<>();
		final List<String> labelledFilterList = new ArrayList<>();

		for(final Field f: field) {
			if (ArcFilter.class.isAssignableFrom(f.getType())) filterList.add(f.getName());
			if (LabelledArcFilter.class.isAssignableFrom(f.getType())) labelledFilterList.add(f.getName());
		}

		final SimpleJSAP jsap = new SimpleJSAP(Transform.class.getName(),
				"Transforms one or more graphs. All transformations require, after the name,\n" +
				"some parameters specified below:\n" +
				"\n" +
				"identity                  sourceBasename destBasename\n" +
				"mapOffline                sourceBasename destBasename map [batchSize] [tempDir] [cutoff]\n" +
				"transposeOffline          sourceBasename destBasename [batchSize] [tempDir]\n" +
				"symmetrizeOffline         sourceBasename destBasename [batchSize] [tempDir]\n" +
						"simplify                  sourceBasename transposeBasename destBasename\n" +
				"simplifyOffline           sourceBasename destBasename [batchSize] [tempDir]\n" +
				"union                     source1Basename source2Basename destBasename [strategy]\n" +
				"compose                   source1Basename source2Basename destBasename [semiring]\n" +
				"gray                      sourceBasename destBasename [batchSize] [tempDir]\n" +
				"grayPerm                  sourceBasename dest\n" +
				"lex                       sourceBasename destBasename [batchSize] [tempDir]\n" +
				"lexPerm                   sourceBasename dest\n" +
				"random                    sourceBasename destBasename [seed] [batchSize] [tempDir]\n" +
				"arcfilter                 sourceBasename destBasename arcFilter (available filters: " + filterList + ")\n" +
				"larcfilter                sourceBasename destBasename arcFilter (available filters: " + labelledFilterList + ")\n" +
				"\n" +
				"Please consult the Javadoc documentation for more information on each transform.",
				new Parameter[] {
						new FlaggedOption("sourceGraphClass", GraphClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "source-graph-class", "Forces a Java class to load the source graph."),
						new FlaggedOption("destGraphClass", GraphClassParser.getParser(), BVGraph.class.getName(), JSAP.NOT_REQUIRED, 'd', "dest-graph-class", "Forces a Java class to store the destination graph."),
						new FlaggedOption("destArcLabelledGraphClass", GraphClassParser.getParser(), BitStreamArcLabelledImmutableGraph.class.getName(), JSAP.NOT_REQUIRED, 'L', "dest-arc-labelled-graph-class", "Forces a Java class to store the labels of the destination graph."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new Switch("offline", 'o', "offline", "Use the offline load method to reduce memory consumption (disables multi-threaded compression)."),
						new Switch("ascii", 'a', "ascii", "Maps are in ASCII form (one integer per line)."),
						new UnflaggedOption("transform", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The transformation to be applied."),
						new UnflaggedOption("param", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The remaining parameters."),
					}
				);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		sourceGraphClass = jsapResult.getClass("sourceGraphClass");
		destGraphClass = jsapResult.getClass("destGraphClass");
		final boolean ascii = jsapResult.getBoolean("ascii");
		final boolean offline = jsapResult.getBoolean("offline");
		final String transform = jsapResult.getString("transform");
		final String[] param = jsapResult.getStringArray("param");

		String source[] = null, dest = null, map = null;
		ArcFilter arcFilter = null;
		LabelledArcFilter labelledArcFilter = null;
		LabelSemiring labelSemiring = null;
		LabelMergeStrategy labelMergeStrategy = null;
		int batchSize = 1000000;
		long cutoff = -1;
		long seed = 0;
		File tempDir = null;

		if (! ensureNumArgs(param, -2)) return;

		if (transform.equals("identity") || transform.equals("grayPerm") || transform.equals("lexPerm")) {
			source = new String[] { param[0] };
			dest = param[1];
			if (! ensureNumArgs(param, 2)) return;
		}
		else if (transform.equals("mapOffline")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0] };
			dest = param[1];
			map = param[2];
			if (param.length >= 4) {
				batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse(param[3])).intValue();
				if (param.length >= 5) {
					tempDir = new File(param[4]);
					if (param.length == 6) cutoff = Long.parseLong(param[5]);
					else if (! ensureNumArgs(param, 5))	return;
				}
				else if (! ensureNumArgs(param, 4))	return;
			}
			else if (! ensureNumArgs(param, 3))	return;
		}
		else if (transform.equals("random")) {
			if (! ensureNumArgs(param, -2)) return;
			source = new String[] { param[0] };
			dest = param[1];
			if (param.length >= 3) {
				seed = Long.parseLong(param[2]);
				if (param.length >= 4) {
					batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse(param[3])).intValue();
					if (param.length == 5) tempDir = new File(param[4]);
					else if (! ensureNumArgs(param, 4))	return;
				}
				else if (! ensureNumArgs(param, 3))	return;
			}
			else if (! ensureNumArgs(param, 2))	return;
		}
		else if (transform.equals("arcfilter")) {
			if (ensureNumArgs(param, 3)) {
				try {
					// First try: a public field
					arcFilter = (ArcFilter) Transform.class.getField(param[2]).get(null);
				}
				catch(final NoSuchFieldException e) {
					// No chance: let's try with a class
					arcFilter = ObjectParser.fromSpec(param[2], ArcFilter.class, GraphClassParser.PACKAGE);
				}
				source = new String[] { param[0], null };
				dest = param[1];
			}
			else return;
		}
		else if (transform.equals("larcfilter")) {
			if (ensureNumArgs(param, 3)) {
				try {
					// First try: a public field
					labelledArcFilter = (LabelledArcFilter) Transform.class.getField(param[2]).get(null);
				}
				catch(final NoSuchFieldException e) {
					// No chance: let's try with a class
					labelledArcFilter = ObjectParser.fromSpec(param[2], LabelledArcFilter.class, GraphClassParser.PACKAGE);
				}
				source = new String[] { param[0], null };
				dest = param[1];
			}
			else return;
		}
		else if (transform.equals("union")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0], param[1] };
			dest = param[2];
			if (param.length == 4) labelMergeStrategy = ObjectParser.fromSpec(param[3], LabelMergeStrategy.class, GraphClassParser.PACKAGE);
			else if (! ensureNumArgs(param, 3)) return;
		}
		else if (transform.equals("compose")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0], param[1] };
			dest = param[2];
			if (param.length == 4) labelSemiring = ObjectParser.fromSpec(param[3], LabelSemiring.class, GraphClassParser.PACKAGE);
			else if (! ensureNumArgs(param, 3)) return;
		}
		else if (transform.equals("simplify")) {
			if (!ensureNumArgs(param, 3)) return;
			source = new String[] { param[0], param[1] };
			dest = param[2];
		}
		else if (transform.equals("transposeOffline") || transform.equals("symmetrizeOffline") || transform.equals("simplifyOffline") || transform.equals("removeDangling") || transform.equals("gray") || transform.equals("lex")) {
			if (! ensureNumArgs(param, -2)) return;
			source = new String[] { param[0] };
			dest = param[1];
			if (param.length >= 3) {
				batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse(param[2])).intValue();
				if (param.length == 4) tempDir = new File(param[3]);
				else if (! ensureNumArgs(param, 3))	return;
			}
			else if (! ensureNumArgs(param, 2))	return;
		}
		else {
			System.err.println("Unknown transform: " + transform);
			return;
		}

		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
		final ImmutableGraph[] graph = new ImmutableGraph[source.length];
		final ImmutableGraph result;
		final Class<?> destLabelledGraphClass = jsapResult.getClass("destArcLabelledGraphClass");
		if (! ArcLabelledImmutableGraph.class.isAssignableFrom(destLabelledGraphClass)) throw new IllegalArgumentException("The arc-labelled destination class " + destLabelledGraphClass.getName() + " is not an instance of ArcLabelledImmutableGraph");

		for (int i = 0; i < source.length; i++)
			// Note that composition requires the second graph to be always random access,
			// and all offline methods will work with an offline graph.
			if (source[i] == null) graph[i] = null;
			else graph[i] = load(sourceGraphClass, source[i], offline && !(i == 1 && transform.equals("compose")), pl);

		final boolean graph0IsLabelled = graph[0] instanceof ArcLabelledImmutableGraph;
		final ArcLabelledImmutableGraph graph0Labelled = graph0IsLabelled ? (ArcLabelledImmutableGraph)graph[0] : null;
		final boolean graph1IsLabelled = graph.length > 1 && graph[1] instanceof ArcLabelledImmutableGraph;

		final String notForLabelled = "This transformation will just apply to the unlabelled graph; label information will be absent";

		if (transform.equals("identity")) result = graph[0];
		else if (transform.equals("mapOffline")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			pl.start("Reading map...");

			final long n = graph[0].numNodes();
			final long[][] f = LongBigArrays.newBigArray(n);
			final long loaded;
			if (ascii) loaded = TextIO.loadLongs(map, f);
			else loaded = BinIO.loadLongs(map, f);

			if(n != loaded) throw new IllegalArgumentException("The source graph has " + n + " nodes, but the permutation contains " + loaded + " longs");

			// Delete from the graph all nodes whose index is above the cutoff, if any.
			if (cutoff != -1)
				for(int i = f.length; i-- != 0;) {
					final long[] t = f[i];
					for(int d = t.length; d-- != 0;) if (t[d] >= cutoff) t[d] = -1;
				}

			pl.count = n;
			pl.done();

			result = mapOffline(graph[0], f, batchSize, tempDir, pl);
			LOGGER.info("Transform computation completed.");
		}
		else if (transform.equals("arcfilter")) {
			if (graph0IsLabelled && ! (arcFilter instanceof LabelledArcFilter)) {
				LOGGER.warn(notForLabelled);
				result = filterArcs(graph[0], arcFilter, pl);
			}
			else result = filterArcs(graph[0], arcFilter, pl);
		}
		else if (transform.equals("larcfilter")) {
			if (! graph0IsLabelled) throw new IllegalArgumentException("Filtering on labelled arcs requires a labelled graph");
			result = filterArcs(graph0Labelled, labelledArcFilter, pl);
		}
		else if (transform.equals("symmetrizeOffline")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = symmetrizeOffline(graph[0], batchSize, tempDir, pl);
		}
		else if (transform.equals("simplifyOffline")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = simplifyOffline(graph[0], batchSize, tempDir, pl);
		}
		else if (transform.equals("removeDangling")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);

			final long n = graph[0].numNodes();
			LOGGER.info("Finding dangling nodes...");

			final long[][] f = LongBigArrays.newBigArray(n);
			final NodeIterator nodeIterator = graph[0].nodeIterator();
			int c = 0;
			for(long i = 0; i < n; i++) {
				nodeIterator.nextLong();
				set(f, i, nodeIterator.outdegree() != 0 ? c++ : -1);
			}
			result = mapOffline(graph[0], f, batchSize, tempDir, pl);
		}
		else if (transform.equals("transposeOffline")) {
			result = graph0IsLabelled ? transposeOffline(graph0Labelled, batchSize, tempDir, pl) : transposeOffline(graph[0], batchSize, tempDir, pl);
		}
		else if (transform.equals("union")) {
			if (graph0IsLabelled && graph1IsLabelled) {
				if (labelMergeStrategy == null) throw new IllegalArgumentException("Uniting labelled graphs requires a merge strategy");
				result = union(graph0Labelled,  (ArcLabelledImmutableGraph)graph[1], labelMergeStrategy);
			}
			else {
				if (graph0IsLabelled || graph1IsLabelled) LOGGER.warn(notForLabelled);
				result = union(graph[0], graph[1]);
			}
		}
		else if (transform.equals("compose")) {
			if (graph0IsLabelled && graph1IsLabelled) {
				if (labelSemiring == null) throw new IllegalArgumentException("Composing labelled graphs requires a composition strategy");
				result = compose(graph0Labelled, (ArcLabelledImmutableGraph)graph[1], labelSemiring);
			}
			else {
				if (graph0IsLabelled || graph1IsLabelled) LOGGER.warn(notForLabelled);
				result = compose(graph[0], graph[1]);
			}
		}
		else if (transform.equals("simplify")) {
			if (graph0IsLabelled || graph1IsLabelled) LOGGER.warn(notForLabelled);
			result = simplify(graph[0], graph[1]);
		}
		else if (transform.equals("gray")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = mapOffline(graph[0], grayCodePermutation(graph[0]), batchSize, tempDir, pl);
		}
		else if (transform.equals("grayPerm")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			BinIO.storeLongs(grayCodePermutation(graph[0]), param[1]);
			return;
		}
		else if (transform.equals("lex")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = mapOffline(graph[0], lexicographicalPermutation(graph[0]), batchSize, tempDir, pl);
		}
		else if (transform.equals("lexPerm")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			BinIO.storeLongs(lexicographicalPermutation(graph[0]), param[1]);
			return;
		}
		else if (transform.equals("random")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = mapOffline(graph[0], randomPermutation(graph[0], seed), batchSize, tempDir, pl);
		} else result = null;

		if (result instanceof ArcLabelledImmutableGraph) {
			// Note that we derelativise non-absolute pathnames to build the underlying graph name.
			LOGGER.info("The result is a labelled graph (class: " + destLabelledGraphClass.getName() + ")");
			final File destFile = new File(dest);
			final String underlyingName = (destFile.isAbsolute() ? dest : destFile.getName()) + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX;
			destLabelledGraphClass.getMethod("store", ArcLabelledImmutableGraph.class, CharSequence.class, CharSequence.class, ProgressLogger.class).invoke(null, result, dest, underlyingName, pl);
			ImmutableGraph.store(destGraphClass, result, dest + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX, pl);
		}
		else ImmutableGraph.store(destGraphClass, result, dest, pl);
	}
}
