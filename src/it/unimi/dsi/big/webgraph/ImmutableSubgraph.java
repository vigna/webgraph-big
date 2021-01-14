/*
 * Copyright (C) 2003-2020 Paolo Boldi and Sebastiano Vigna
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

import static it.unimi.dsi.fastutil.BigArrays.get;
import static it.unimi.dsi.fastutil.BigArrays.length;
import static it.unimi.dsi.fastutil.BigArrays.set;

import java.io.DataInput;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

/**
 * An induced subgraph of a given immutable graph.
 *
 * <P>
 * <strong>Warning</strong>: this class is experimental, and might be subject to changes.
 *
 * <P>
 * The nodes of the subgraph are specified via an {@link it.unimi.dsi.fastutil.longs.LongSet} or an
 * array of integers. Of course, each node in the subgraph will have a different index than the
 * corresponding node in the supergraph. The two methods {@link #toSupergraphNode(long)} and
 * {@link #fromSupergraphNode(long)} are used to translate indices back and forth.
 *
 * <P>
 * An immutable subgraph is stored as a property file (which follows the convention established in
 * {@link it.unimi.dsi.big.webgraph.ImmutableGraph}), and as a node subset file. The latter must
 * contain an (increasing) list of longs in {@link java.io.DataOutput} format representing the set
 * of nodes of the subgraph.
 *
 * <P>
 * The property file, with named <code><var>basename</var>.properties</code>, contains the following
 * entries:
 * <UL>
 * <LI><code>supergraphbasename</code>: the basename of the supergraph. Note that this name is
 * system-dependent: it is suggested that you use a path-free filename.
 * <LI><code>subgraphnodes</code>: the filename of the node subset file, which must be an list of
 * longs in {@link DataInput} format. If this property is not present, it is assumed to be
 * <code><var>basename</var>.subgraphnodes</code>.
 * </UL>
 *
 * <P>
 * You can create an immutable subgraph using the public constructor, or you can load one using one
 * of the provided load methods. Note that there is no <code>store</code> method, because it makes
 * no sense to store a generic {@link it.unimi.dsi.big.webgraph.ImmutableGraph} as a subgraph. There
 * is, however, a {@linkplain #save(CharSequence, ProgressLogger) save method} that allows one to
 * save the files related to a subgraph (the property file and the subgraph node file).
 *
 * <H2>Root graphs</H2>
 *
 * <P>
 * When creating tree-shaped hierarchies of subgraphs, the methods {@link #rootBasename()},
 * {@link #fromRootNode(long)} and {@link #toRootNode(long)} may be used to access information about
 * the root (i.e., the unique highest graph in the hierarchy: note that it cannot be an
 * <code>ImmutableSubgraph</code>).
 *
 * <P>
 * Should you need to treat uniformly a generic immutable graph as an immutable subgraph, the method
 * {@link #asImmutableSubgraph(ImmutableGraph)} will return a subgraph view of the given immutable
 * graph in which all to/from functions are identities.
 */
public class ImmutableSubgraph extends ImmutableGraph {
	private final static boolean DEBUG = false;

	/** The standard property key for the name of the file containing the subgraph nodes. */
	public static final String SUBGRAPHNODES_PROPERTY_KEY = "subgraphnodes";
	/** The standard property key for the supergraph basename. */
	public static final String SUPERGRAPHBASENAME_PROPERTY_KEY = "supergraphbasename";

	/** The supergraph. */
	final protected ImmutableGraph supergraph;

	/** If {@link #supergraph} is an instance of {@link ImmutableSubgraph}, it is cached here. */
	protected final ImmutableSubgraph supergraphAsSubgraph;

	/** The nodes of the subgraph, in increasing order. */
	protected final long[][] subgraphNode;

	/** A mapping from nodes of the supergraph to nodes in the subgraph (-1 for missing nodes). */
	protected final long[][] supergraphNode;

	/** The number of nodes in the subgraph. */
	protected final long subgraphSize;

	/** The number of nodes in the supergraph. */
	protected final long supergraphNumNodes;

	/** The basename of this immutable subgraph, if it was loaded from disk, or <code>null</code>. */
	protected CharSequence basename;

	/**
	 * Creates a new immutable subgraph using a given backing node array.
	 *
	 * <P>
	 * Note that <code>subgraphNode</code> is <em>not</em> copied: thus, it must not be modified until
	 * this subgraph is no longer in use.
	 *
	 * @param supergraph the supergraph.
	 * @param subgraphNode an increasing array containing the nodes defining the induced subgraph.
	 */
	public ImmutableSubgraph(final ImmutableGraph supergraph, final long[][] subgraphNode) {
		this.supergraph = supergraph;
		this.supergraphAsSubgraph = supergraph instanceof ImmutableSubgraph ? (ImmutableSubgraph)supergraph : null;
		this.subgraphNode = subgraphNode;
		this.subgraphSize = length(subgraphNode);
		this.supergraphNumNodes = supergraph.numNodes();
		this.supergraphNode = LongBigArrays.newBigArray(supergraphNumNodes);
		BigArrays.fill(supergraphNode, -1L);
		for (long i = subgraphSize; i-- != 0;) set(supergraphNode, get(subgraphNode, i), i);
		for (long i = 1; i < subgraphSize; i++) if (get(subgraphNode, i - 1) >= get(subgraphNode, i)) throw new IllegalArgumentException("The provided integer array is not strictly increasing: " + (i - 1) + "-th element is " + get(subgraphNode, i - 1) + ", " + i + "-th element is " + get(subgraphNode, i));
		if (subgraphSize > 0 && get(subgraphNode, subgraphSize - 1) >= supergraphNumNodes) throw new IllegalArgumentException("Subnode index out of bounds: " + get(subgraphNode, subgraphSize - 1));
	}

	/**
	 * Creates a new immutable subgraph by copying an existing one.
	 *
	 * @param immutableSubgraph an immutable subgraph.
	 */
	protected ImmutableSubgraph(final ImmutableSubgraph immutableSubgraph) {
		this.supergraphNumNodes = immutableSubgraph.supergraphNumNodes;
		this.subgraphSize = immutableSubgraph.subgraphSize;
		this.supergraph = immutableSubgraph.supergraph.copy();
		this.supergraphAsSubgraph = supergraph instanceof ImmutableSubgraph ? (ImmutableSubgraph)supergraph : null;
		this.subgraphNode = immutableSubgraph.subgraphNode;
		this.supergraphNode = immutableSubgraph.supergraphNode;
	}

	/**
	 * Creates a new immutable subgraph by wrapping an immutable graph.
	 *
	 * @param immutableGraph an immutable graph.
	 */
	protected ImmutableSubgraph(final ImmutableGraph immutableGraph) {
		this.subgraphSize = this.supergraphNumNodes = immutableGraph.numNodes();
		this.supergraph = immutableGraph;
		this.supergraphAsSubgraph = null;
		this.subgraphNode = this.supergraphNode = null;
	}

	@Override
	public long numNodes() {
		return subgraphSize;
	}

	@Override
	public long numArcs() {
		throw new UnsupportedOperationException("Cannot determine the number of arcs in a subgraph");
	}

	@Override
	public boolean randomAccess() {
		return supergraph.randomAccess();
	}

	@Override
	public boolean hasCopiableIterators() {
		return supergraph.hasCopiableIterators();
	}

	@Override
	public CharSequence basename() {
		if (basename == null) throw new IllegalStateException("This immutable subgraph has no basename");
		return basename;
	}

	/**
	 * Returns the basename of the root graph.
	 *
	 * @return the {@linkplain ImmutableGraph#basename() basename} of the root graph.
	 */
	public CharSequence rootBasename() {
		return supergraphAsSubgraph != null ? supergraphAsSubgraph.rootBasename() : supergraph.basename();
	}

	/**
	 * Returns the index of a node of this graph in its supergraph.
	 *
	 * @param x an index of a node in this graph.
	 * @return the index of node <code>x</code> in the supergraph.
	 */
	public long toSupergraphNode(final long x) {
		if (x < 0 || x >= subgraphSize) throw new IllegalArgumentException();
		return get(subgraphNode, x);
	}

	/**
	 * Returns the index of a node of the supergraph in this graph.
	 *
	 * @param x an index of a node in the supergraph.
	 * @return the index of node <code>x</code> in this graph, or a negative value if <code>x</code>
	 *         does not belong to the subgraph.
	 */
	public long fromSupergraphNode(final long x) {
		return get(supergraphNode, x);
	}

	/**
	 * Returns the index of a node of this graph in its root graph.
	 *
	 * @param x an index of a node in this graph.
	 * @return the index of node <code>x</code> in the root graph.
	 */
	public long toRootNode(final long x) {
		return supergraphAsSubgraph != null ? supergraphAsSubgraph.toRootNode(toSupergraphNode(x)) : toSupergraphNode(x);
	}

	/**
	 * Returns the index of a node of the root graph in this graph.
	 *
	 * @param x an index of a node in the root graph.
	 * @return the index of node <code>x</code> in this graph, or a negative value if <code>x</code>
	 *         does not belong to the root graph.
	 */
	public long fromRootNode(final long x) {
		if (supergraphAsSubgraph == null) return fromSupergraphNode(x);
		final long y = supergraphAsSubgraph.fromRootNode(x);
		if (y < 0) return -1;
		return fromSupergraphNode(y);
	}

	/**
	 * If this variable is non-negative, we are caching the successors' array of node
	 * <code>cacheNode</code> (in the subgraph).
	 */
	private long cacheNode = -1;

	/**
	 * If <code>cacheNode</code>&gt; 0, this array contains the successors of node
	 * <code>cacheNode</code> (in the subgraph).
	 */
	private long[][] cacheSuccessors;

	@Override
	public NodeIterator nodeIterator(final long from) {
		/**
		 * The invariant that we are assuming here is the following: at any time, <code>node</code> is the
		 * next (subgraph) node to be returned by {@link #nextInt()}. This variable contain sensible data
		 * only when <code>node</code> &lt; <code>subgraphSize</code>. Moreover, if outdegree >= 0 then it
		 * is the outdegree of <code>node</code>-1, and <code>successorsCache</code> contains the
		 * successors.
		 */

		return new ImmutableSubgraphNodeIterator(from, Integer.MAX_VALUE);
	}

	@Override
	public LazyLongIterator successors(final long x) {
		return successors(x, supergraph.successors(toSupergraphNode(x)));
	}

	private LazyLongIterator successors(final long x, final LazyLongIterator supergraphSuccessors) {
		if (DEBUG) System.err.println(this.getClass().getName() + ".successors(" + x + ", " + supergraphSuccessors + ")");

		if (x < 0 || x >= subgraphSize) throw new IllegalArgumentException();
		if (cacheNode == x) return LazyLongIterators.wrap(cacheSuccessors);

		if (DEBUG) System.err.println(this.getClass().getName() + ": returning new iterator");

		return new LazyLongIterator() {

			@Override
			public long nextLong() {
				long x, result;
				while ((x = supergraphSuccessors.nextLong()) != -1) {
					result = get(supergraphNode, x);
					if (result >= 0) return result;
				}

				return -1;
			}

			@Override
			public long skip(final long n) {
				long i;
				for (i = 0; i < n && nextLong() != -1; i++);
				return i;
			}
		};
	}

	@Override
	public long outdegree(final long x) {
		return outdegree(x, supergraph.successors(toSupergraphNode(x)));
	}

	public long outdegree(final long x, final LazyLongIterator supergraphSuccessors) {
		if (x < 0 || x >= subgraphSize) throw new IllegalArgumentException();
		if (cacheNode == x) return length(cacheSuccessors);
		// TODO: this is not really efficient--we should reuse the cache.
		cacheSuccessors = LazyLongIterators.unwrap(successors(x, supergraphSuccessors));
		cacheNode = x;
		assert cacheSuccessors != null;
		return length(cacheSuccessors);
	}

	private final class ImmutableSubgraphNodeIterator extends NodeIterator {
		private final long from;
		/** The current node (the next to be returned). */
		long node;
		/** This array caches the successors of the node that was returned last (<code>from</code>-1). */
		long[][] successorsCache = LongBigArrays.EMPTY_BIG_ARRAY;
		/** The outdegree of the node that was returned last (<code>node</code>-1). */
		long outdegree = -1;
		final NodeIterator supergraphNodeIterator;
		/** No node &ge; this will ever be returned. */
		final long upperBound;

		private ImmutableSubgraphNodeIterator(final long from, final long to) {
			this.from = from;
			node = from;
			supergraphNodeIterator = supergraph.nodeIterator(get(subgraphNode, from));
			upperBound = to;
		}

		@Override
		public long nextLong() {
			if (!hasNext()) throw new java.util.NoSuchElementException();
			if (node != from) supergraphNodeIterator.skip(get(subgraphNode, node) - get(subgraphNode, node - 1));
			else supergraphNodeIterator.nextLong();
			outdegree = -1;
			return node++;
		}

		@Override
		public boolean hasNext() {
			return node < Math.min(subgraphSize, upperBound);
		}

		private void unwrapSuccessors() {
			long start = 0, done;
			final LazyLongIterator i = ImmutableSubgraph.this.successors(node - 1, supergraphNodeIterator.successors());
			// ALERT: we removed i.hasNext() at the end of this check, but it is not necessary
			while ((done = LazyLongIterators.unwrap(i, successorsCache, start, length(successorsCache) - start)) == length(successorsCache) - start) {
				start = length(successorsCache);
				successorsCache = BigArrays.grow(successorsCache, length(successorsCache) + 1);
			}
			outdegree = start + done;
		}

		@Override
		public LazyLongIterator successors() {
			if (node == from) throw new IllegalStateException();
			if (outdegree == -1) unwrapSuccessors();
			return LazyLongIterators.wrap(successorsCache, outdegree);
		}

		@Override
		public long outdegree() {
			if (node == from) throw new IllegalStateException();
			if (outdegree == -1) unwrapSuccessors();
			return outdegree;
		}

		@Override
		public NodeIterator copy(final long upperBound) {
			final ImmutableSubgraphNodeIterator result = new ImmutableSubgraphNodeIterator(from, upperBound);
			result.node = node;
			result.outdegree = outdegree;
			return result;
		}

	}

	/**
	 * A wrapper for immutable graphs, which exhibits them as immutable subgraphs. Essentially, all
	 * functions concerning supergraphs are defined as identities.
	 */

	private static class ImmutableGraphWrapper extends ImmutableSubgraph {

		public ImmutableGraphWrapper(final ImmutableGraph graph) {
			super(graph);
			try {
				basename = graph.basename();
			} catch (final UnsupportedOperationException e) {
				basename = null;
			}
		}

		@Override
		public NodeIterator nodeIterator() {
			return supergraph.nodeIterator();
		}

		@Override
		public NodeIterator nodeIterator(final long from) {
			return supergraph.nodeIterator(from);
		}

		@Override
		public long numArcs() {
			return supergraph.numArcs();
		}

		@Override
		public long numNodes() {
			return supergraph.numNodes();
		}

		@Override
		public long outdegree(final long x) {
			return supergraph.outdegree(x);
		}

		@Override
		public LazyLongIterator successors(final long x) {
			return supergraph.successors(x);
		}

		@Override
		public long toSupergraphNode(final long x) {
			return x;
		}

		@Override
		public long fromSupergraphNode(final long x) {
			return x;
		}

		@Override
		public long toRootNode(final long x) {
			return x;
		}

		@Override
		public long fromRootNode(final long x) {
			return x;
		}
	}

	@Override
	public ImmutableSubgraph copy() {
		return new ImmutableSubgraph(this);
	}

	/**
	 * Returns a subgraph view of the given immutable graph.
	 *
	 * <P>
	 * The wrapper returned by this method may be used whenever immutable graphs and subgraphs must be
	 * mixed.
	 *
	 * @param graph an immutable graph.
	 * @return the given graph, viewed as a trivial subgraph of itself.
	 */
	public static ImmutableSubgraph asImmutableSubgraph(final ImmutableGraph graph) {
		return new ImmutableGraphWrapper(graph);
	}

	@Deprecated
	public static ImmutableGraph loadSequential(final CharSequence basename) throws IOException {
		return load(LoadMethod.STANDARD, basename); // TODO: is this what we really want?
	}

	@Deprecated
	public static ImmutableGraph loadSequential(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.STANDARD, basename, pl);
	}

	public static ImmutableGraph loadOffline(final CharSequence basename) throws IOException {
		return load(LoadMethod.OFFLINE, basename);
	}

	public static ImmutableGraph loadOffline(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.OFFLINE, basename, pl);
	}

	public static ImmutableGraph load(final CharSequence basename) throws IOException {
		return load(LoadMethod.STANDARD, basename);
	}

	public static ImmutableGraph load(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.STANDARD, basename, pl);
	}

	private static ImmutableGraph load(final LoadMethod method, final CharSequence basename) throws IOException {
		return load(method, basename, null);
	}

	public static ImmutableGraph loadMapped(final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load(LoadMethod.MAPPED, basename, pl);
	}

	public static ImmutableGraph loadMapped(final CharSequence basename) throws IOException {
		return load(LoadMethod.MAPPED, basename, null);
	}

	/**
	 * Creates a new immutable subgraph by loading the supergraph, delegating the actual loading to the
	 * class specified in the <code>supergraphclass</code> property within the property file (named
	 * <code><var>basename</var>.properties</code>), and loading the subgraph array in memory. The exact
	 * load method to be used depends on the <code>method</code> argument.
	 *
	 * @param method the method to be used to load the supergraph.
	 * @param basename the basename of the graph.
	 * @param pl the progress logger; it can be <code>null</code>.
	 * @return an immutable subgraph containing the specified graph.
	 */

	protected static ImmutableGraph load(final LoadMethod method, final CharSequence basename, final ProgressLogger pl) throws IOException {
		final FileInputStream propertyFile = new FileInputStream(basename + PROPERTIES_EXTENSION);
		final Properties properties = new Properties();
		properties.load(propertyFile);
		propertyFile.close();

		final String graphClassName = properties.getProperty(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY);
		if (!graphClassName.equals(ImmutableSubgraph.class.getName())) throw new IOException("This class (" + ImmutableSubgraph.class.getName() + ") cannot load a graph stored using " + graphClassName);

		final String supergraphBasename = properties.getProperty(SUPERGRAPHBASENAME_PROPERTY_KEY);
		if (supergraphBasename == null) throw new IOException("This property file does not specify the required property supergraphbasename");

		final ImmutableGraph supergraph = ImmutableGraph.load(method, supergraphBasename, null, pl);

		if (pl != null) pl.start("Reading nodes...");
		final String nodes = properties.getProperty(SUBGRAPHNODES_PROPERTY_KEY);
		final ImmutableSubgraph isg = new ImmutableSubgraph(supergraph, BinIO.loadLongsBig(nodes != null ? nodes : basename + ".nodes"));
		if (pl != null) {
			pl.count = isg.numNodes();
			pl.done();
		}
		isg.basename = new MutableString(basename);
		return isg;
	}

	/** Throws an {@link UnsupportedOperationException}. */
	@SuppressWarnings("unused")
	public static void store(final ImmutableGraph graph, final CharSequence basename, final ProgressLogger pm) {
		throw new UnsupportedOperationException("You cannot store a generic immutable graph as a subgraph");
	}

	/** Throws an {@link UnsupportedOperationException}. */
	public static void store(final ImmutableGraph graph, final CharSequence basename) {
		store(graph, basename, (ProgressLogger)null);
	}

	/**
	 * Saves this immutable subgraph with a given basename.
	 *
	 * <P>
	 * Note that this method will <strong>not</strong> save the supergraph, but only the subgraph files,
	 * that is, the subgraph property file (with extension <code>.properties</code>) and the file
	 * containing the subgraph nodes (with extension <code>.nodes</code>). A reference to the supergraph
	 * basename will be stored in the property file.
	 *
	 * @param basename the basename to be used to save the subgraph.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public void save(final CharSequence basename, final ProgressLogger pl) throws IOException {

		final Properties properties = new Properties();
		properties.setProperty(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY, ImmutableSubgraph.class.getName());
		properties.setProperty(SUPERGRAPHBASENAME_PROPERTY_KEY, supergraph.basename().toString());

		final FileOutputStream propertyFile = new FileOutputStream(basename + PROPERTIES_EXTENSION);
		properties.store(propertyFile, null);
		propertyFile.close();

		// Save the subgraph nodes
		if (pl != null) pl.start("Saving nodes...");
		BinIO.storeLongs(subgraphNode, 0, length(subgraphNode), basename + ".nodes");

		if (pl != null) {
			pl.count = length(subgraphNode);
			pl.done();
		}
	}

	public void save(final CharSequence basename) throws IOException {
		save(basename, (ProgressLogger)null);
	}

	public static void main(final String args[]) throws IllegalArgumentException, SecurityException, JSAPException, UnsupportedEncodingException, FileNotFoundException {

		final SimpleJSAP jsap = new SimpleJSAP(ImmutableSubgraph.class.getName(), "Writes the property file of an immutable subgraph.", new Parameter[] {
				new UnflaggedOption("supergraphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the supergraph."),
				new FlaggedOption("subgraphNodes", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 's', "subgraph-nodes", "Sets a subgraph node file (a list integers in DataInput format). If not specified, the name will be stemmed from the basename."),
				new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of resulting immutable subgraph."), });

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(jsapResult.getString("basename") + ImmutableGraph.PROPERTIES_EXTENSION), "UTF-8"));
		pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + ImmutableSubgraph.class.getName());
		pw.println("supergraphbasename = " + jsapResult.getString("supergraphBasename"));
		if (jsapResult.userSpecified("subgraphNodes")) pw.println("subgraphnodes = " + jsapResult.getString("subgraphNodes"));
		pw.close();
	}
}
