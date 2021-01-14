/*
 * Copyright (C) 2020 Paolo Boldi and Sebastiano Vigna
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

package it.unimi.dsi.big.webgraph.typed;

import static it.unimi.dsi.big.webgraph.typed.TypedGraph.id;
import static it.unimi.dsi.big.webgraph.typed.TypedGraph.node;
import static it.unimi.dsi.big.webgraph.typed.TypedGraph.type;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.util.NoSuchElementException;

import org.apache.commons.configuration2.ex.ConfigurationException;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.LazyLongIterators;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.ByteBufferInputStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Properties;

public class BVImmutableTypedGraph implements TypedGraph, Closeable, FlyweightPrototype<BVImmutableTypedGraph> {
	/** The type graph, as provided in the constructor. */
	private final ImmutableGraph immutableTypeGraph;
	/** The number of types. */
	private final int numTypes;
	/** A fast-access representation of the type graph. */
	private final int[][] typeGraph;
	/** For each type, the number of nodes. */
	private final long[] numNodes;
	/** For each pair of types, the number of arcs. */
	private final long[][] numArcs;
	/** For each type, the random access file used to map the graph stream. */
	private final RandomAccessFile[] graphRandomAccessFile;
	/**
	 * For each type, the offsets ({@code null} if the associated {@link #graphRandomAccessFile} is
	 * empty).
	 */
	private final LongBigList[] offset;
	/**
	 * For each type, a graph stream ({@code null} if the associated {@link #graphRandomAccessFile} is
	 * empty)
	 */
	private final ByteBufferInputStream graphStream[];

	private static ByteBufferInputStream[] getGraphStreams(final ImmutableGraph immutableTypeGraph, final RandomAccessFile[] graphRandomAccessFile) throws IOException {
		final int numTypes = (int)immutableTypeGraph.numNodes();
		final ByteBufferInputStream[] graphStream = new ByteBufferInputStream[numTypes];
		for(int i = 0; i < numTypes; i++)
			if (graphRandomAccessFile[i].length() != 0) graphStream[i] = ByteBufferInputStream.map(graphRandomAccessFile[i].getChannel(), MapMode.READ_ONLY);
		return graphStream;
	}

	protected BVImmutableTypedGraph(final ImmutableGraph immutableTypeGraph, final long[] numNodes, final long[][] numArcs, final RandomAccessFile[] graphRandomAccessFile, final LongBigList[] offset) throws IOException {
		this(immutableTypeGraph, numNodes, numArcs, graphRandomAccessFile, getGraphStreams(immutableTypeGraph, graphRandomAccessFile), offset);
	}

	protected BVImmutableTypedGraph(final ImmutableGraph immutableTypeGraph, final long[] numNodes, final long[][] numArcs, final RandomAccessFile[] graphRandomAccessFile, final ByteBufferInputStream[] graphStream, final LongBigList[] offset) {
		this.immutableTypeGraph = immutableTypeGraph;
		this.numTypes = (int)immutableTypeGraph.numNodes();
		this.typeGraph = new int[numTypes][];
		for(int i = 0; i < numTypes; i++) {
			this.typeGraph[i] = new int[(int)immutableTypeGraph.outdegree(i)];
			final LazyLongIterator successors = immutableTypeGraph.successors(i);
			for (int s, j = 0; (s = (int)successors.nextLong()) != -1; j++) this.typeGraph[i][j] = s;
		}
		this.numNodes = numNodes;
		this.numArcs = numArcs;
		this.graphStream = graphStream;
		this.graphRandomAccessFile = graphRandomAccessFile;
		this.offset = offset;
	}

	@Override
	public void close() throws IOException {
		for (final RandomAccessFile r : graphRandomAccessFile) r.close();
	}

	@Override
	public ImmutableGraph typeGraph() {
		return immutableTypeGraph;
	}

	@Override
	public long numNodes() {
		long tot = 0;
		for (final long t : numNodes) tot += t;
		return tot;
	}

	@Override
	public long numNodes(final int type) {
		return numNodes[type];
	}

	@Override
	public long numArcs() {
		long tot = 0;
		for (final long[] t : numArcs) for (final long u : t) tot += u;
		return tot;

	}

	@Override
	public long outdegree(final long node) throws IOException {
		final int type = type(node);
		final long id = id(node);
		if (type >= numTypes || id >= numNodes[type]) throw new IndexOutOfBoundsException();
		if (graphStream[type] == null) return 0;
		@SuppressWarnings("resource")
		final InputBitStream ibs = new InputBitStream(graphStream[type].copy());
		final int outTypes = typeGraph[type].length;
		ibs.position(offset[type].getLong(id));
		long totd = 0;
		for (int i = 0; i < outTypes; i++) totd += (ibs.readLongGamma());
		return totd;
	}

	@Override
	public LazyLongIterator successors(final long node) throws IOException {
		final int type = type(node);
		final long id = id(node);
		if (id >= numNodes[type]) throw new IndexOutOfBoundsException();
		if (graphStream[type] == null) return LazyLongIterators.EMPTY_ITERATOR;
		@SuppressWarnings("resource")
		final InputBitStream ibs = new InputBitStream(graphStream[type].copy());
		final int outTypes = typeGraph[type].length;
		final long[] d = new long[outTypes];
		ibs.position(offset[type].getLong(id));
		long _totd = 0;
		for (int i = 0; i < outTypes; i++) _totd += (d[i] = ibs.readLongGamma());
		final long totd = _totd;
		final int[] succTypes = typeGraph[type];

		return new LazyLongIterator() {
			private long i = 0;
			private long pos = 0;
			private int succTypePos = 0;
			private int succType = succTypes[succTypePos];
			private long prevId = -1;

			@Override
			public long nextLong() {
				if (i == totd) return -1;
				while (pos >= d[succTypePos]) {
					succType = succTypes[++succTypePos];
					prevId = -1;
					pos = 0;
				}
				i++;
				pos++;
				long next;
				try {
					next = ibs.readLongGamma() + prevId + 1;
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
				prevId = next;
				return node(succType, next);
			}

			@Override
			public long skip(final long n) {
				long i = 0;
				for (i = 0; i < n; i++) if (nextLong() == -1) break;
				return i;
			}
		};
	}

	@Override
	public NodeIterator nodeIterator() {
		return new NodeIterator() {
			final long n = numNodes();
			long i = 0;
			long id = 0;
			int type = 0;

			@Override
			public long nextLong() {
				if (! hasNext()) throw new NoSuchElementException();
				while (id >= numNodes[type]) {
					type++;
					id = 0;
				}
				final long node = node(type, id);
				i++;
				id++;
				return node;
			}

			@Override
			public boolean hasNext() {
				return i < n;
			}

			@Override
			public long outdegree() {
				try {
					return BVImmutableTypedGraph.this.outdegree(node(type, id));
				} catch (final IOException e) {
					throw new RuntimeException();
				}
			}

			@Override
			public LazyLongIterator successors() {
				try {
					return BVImmutableTypedGraph.this.successors(node(type, id));
				} catch (final IOException e) {
					throw new RuntimeException();
				}
			}

		};
	}

	public static BVImmutableTypedGraph load(final CharSequence basename, final CharSequence typeBasename, @SuppressWarnings("unused") final ProgressLogger pl) throws IOException, ClassNotFoundException, ConfigurationException {
		final Properties properties = new Properties(basename + BVGraph.PROPERTIES_EXTENSION);
		String[] t = properties.getStringArray("nodes");
		final int numTypes = t.length;
		final long[] numNodes = new long[numTypes];
		for (int i = 0; i < numTypes; i++) numNodes[i] = Long.parseLong(t[i]);

		t = properties.getStringArray("arcs");
		final long[][] numArcs = new long[numTypes][numTypes];
		for (int i = 0; i < numTypes * numTypes; i++) numArcs[i / numTypes][i % numTypes] = Long.parseLong(t[i]);

		final ImmutableGraph typeGraph = ImmutableGraph.load(typeBasename);
		final RandomAccessFile[] graphRandomAccessFile = new RandomAccessFile[numTypes];
		final LongBigList[] offset = new LongBigList[numTypes];

		for (int i = 0; i < numTypes; i++) {
			graphRandomAccessFile[i] = new RandomAccessFile(basename + "." + i + BVGraph.GRAPH_EXTENSION, "r");
			// Do not load offsets for types without successors
			if (graphRandomAccessFile[i].length() != 0) offset[i] = (LongBigList)BinIO.loadObject(basename + "." + i + BVGraph.OFFSETS_BIG_LIST_EXTENSION);
		}
		return new BVImmutableTypedGraph(typeGraph, numNodes, numArcs, graphRandomAccessFile, offset);
	}

	@Override
	public BVImmutableTypedGraph copy() {
		return new BVImmutableTypedGraph(immutableTypeGraph, numNodes, numArcs, graphRandomAccessFile, graphStream, offset);
	}
}
