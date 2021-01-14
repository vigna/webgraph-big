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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.stat.SummaryStats;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList16;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;

public class ConvertToTypedGraph {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConvertToTypedGraph.class);

	/** The standard extension for file of ids. */
	public static final String IDS_EXTENSION = ".ids";

	/** An iterator returning the offsets. */
	private final static class OffsetsLongIterator implements LongIterator {
		private final InputBitStream offsetIbs;
		private final long n;
		private long off;
		private long i;

		private OffsetsLongIterator(final long numNodes, final InputBitStream offsetIbs) {
			this.offsetIbs = offsetIbs;
			this.n = numNodes;
		}

		@Override
		public boolean hasNext() {
			return i <= n;
		}

		@Override
		public long nextLong() {
			i++;
			try {
				return off = offsetIbs.readLongDelta() + off;
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static Map<String, SummaryStats> convert(final ImmutableGraph graph, final ImmutableGraph typeGraph, final LongBigList nodeTypes, final String basename, final ProgressLogger pl) throws IOException {
		final Object2ObjectOpenHashMap<String, SummaryStats> data = new Object2ObjectOpenHashMap<>();
		final long[][] id = LongBigArrays.newBigArray(graph.numNodes());
		final int numTypes = (int)typeGraph.numNodes();
		final long[] count = new long[numTypes];

		final SummaryStats bitsForOutdegrees = new SummaryStats(), bitsForDeltas = new SummaryStats(),
				emptyLists = new SummaryStats();
		data.put("bitsForOutdegrees", bitsForOutdegrees);
		data.put("bitsForDeltas", bitsForDeltas);
		data.put("emptyLists", emptyLists);

		final long[] numNodes = new long[numTypes];
		final long[][] numArcs = new long[numTypes][numTypes];

		for (long node = 0; node < graph.numNodes(); node++) {
			final int type = (int)nodeTypes.getLong(node);
			BigArrays.set(id, node, count[type]++);
			numNodes[type]++;
		}

		final LongArrayList[] succ = new LongArrayList[numTypes];
		for (int i = 0; i < succ.length; i++) succ[i] = new LongArrayList();

		if (pl != null) {
			pl.itemsName = "arcs";
			pl.expectedUpdates = graph.numArcs();
			pl.start("Analyzing...");
		}

		final OutputBitStream[] offsets = new OutputBitStream[numTypes];
		for (int i = 0; i < numTypes; i++) {
			offsets[i] = new OutputBitStream(basename + "." + i + BVGraph.OFFSETS_EXTENSION);
			offsets[i].writeLongDelta(0);
		}
		final long[] bitOffset = new long[numTypes];

		final OutputBitStream[] outputBitStream = new OutputBitStream[numTypes];
		for (int i = 0; i < numTypes; i++) outputBitStream[i] = new OutputBitStream(basename + "." + i + BVGraph.GRAPH_EXTENSION);

		for (final NodeIterator nodeIterator = graph.nodeIterator(); nodeIterator.hasNext();) {
			final long source = nodeIterator.nextLong();
			if ((source & 0x7FFFFFF) == 0) LOGGER.info(data.toString());
			final int sourceType = (int)nodeTypes.getLong(source);
			final LazyLongIterator successors = nodeIterator.successors();
			long target;
			while ((target = successors.nextLong()) != -1) {
				final int targetType = (int)nodeTypes.getLong(target);
				succ[targetType].add(target);
				numArcs[sourceType][targetType]++;
			}

			LazyLongIterator successorTypes = typeGraph.successors(sourceType);
			int succType;
			// Write outdegrees first
			while ((succType = (int)successorTypes.nextLong()) != -1) bitsForOutdegrees.add(outputBitStream[sourceType].writeLongGamma(succ[succType].size()));

			// Write successor lists
			successorTypes = typeGraph.successors(sourceType);
			while ((succType = (int)successorTypes.nextLong()) != -1) {
				long prev = -1;
				for (final long node : succ[succType]) {
					final long t = BigArrays.get(id, node);
					bitsForDeltas.add(outputBitStream[sourceType].writeLongGamma(t - prev - 1));
					prev = t;
				}

				succ[succType].clear();
			}

			offsets[sourceType].writeLongDelta(outputBitStream[sourceType].writtenBits() - bitOffset[sourceType]);
			bitOffset[sourceType] = outputBitStream[sourceType].writtenBits();

			for (final LongArrayList l : succ) {
				if (!l.isEmpty()) {
					LOGGER.error("Detected arc " + source + " -> [" + l.getLong(0) + "] from type " + sourceType + " to type " + nodeTypes.getLong(l.getLong(0)));
					l.clear();
				}
			}

			if (pl != null) pl.update(nodeIterator.outdegree());
		}

		if (pl != null) pl.done();

		for (final OutputBitStream o : offsets) o.close();
		for(final OutputBitStream o : outputBitStream) o.close();

		// Convert all .offsets files to .obl
		for (int i = 0; i < numTypes; i++) {
			final InputBitStream off = new InputBitStream(basename + "." + i + BVGraph.OFFSETS_EXTENSION);
			final long upperBound = new File(basename + "." + i + BVGraph.GRAPH_EXTENSION).length() * Byte.SIZE + 1;
			BinIO.storeObject(EliasFanoMonotoneLongBigList.fits(numNodes[i] + 1, upperBound) ? new EliasFanoMonotoneLongBigList(numNodes[i] + 1, upperBound, new OffsetsLongIterator(numNodes[i], off)) : new EliasFanoMonotoneLongBigList16(graph.numNodes() + 1, upperBound, new OffsetsLongIterator(numNodes[i], off)), basename + "." + i + BVGraph.OFFSETS_BIG_LIST_EXTENSION);
			off.close();
		}

		BinIO.storeLongs(id, basename + IDS_EXTENSION);
		final Properties properties = new Properties();
		properties.setProperty("nodes", StringUtils.join(numNodes, ','));
		final StringBuilder arcsString = new StringBuilder();
		for (int i = 0; i < numTypes; i++) {
			if (arcsString.length() != 0) arcsString.append(',');
			arcsString.append(StringUtils.join(numArcs[i], ','));
		}
		properties.setProperty("arcs", arcsString.toString());
		final FileOutputStream propertyFile = new FileOutputStream(basename + BVGraph.PROPERTIES_EXTENSION);
		properties.store(propertyFile, "BVTypedGraph properties");
		return data;
	}

	public static boolean verify(final ImmutableGraph graph, final ImmutableGraph typeGraph, final LongBigList nodeTypes, final long[][] id, final BVImmutableTypedGraph typedGraph, final ProgressLogger pl) throws IOException {
		boolean result = true;
		final int numTypes = (int)typeGraph.numNodes();

		final LongArrayList[] succ = new LongArrayList[numTypes];
		for (int i = 0; i < succ.length; i++) succ[i] = new LongArrayList();

		if (pl != null) {
			pl.itemsName = "arcs";
			pl.expectedUpdates = graph.numArcs();
			pl.start("Verifying...");
		}

		for (final NodeIterator nodeIterator = graph.nodeIterator(); nodeIterator.hasNext();) {
			final long source = nodeIterator.nextLong();
			final int sourceType = (int)nodeTypes.getLong(source);
			final long sourceId = BigArrays.get(id, source);
			final long typedSource = node(sourceType, sourceId);

			if (nodeIterator.outdegree() != typedGraph.outdegree(typedSource)) {
				LOGGER.error("Wrong outdegree for node " + source + "(type " + sourceType + ", id " + sourceId + "):" + nodeIterator.outdegree() + " != " + typedGraph.outdegree(typedSource));
				result = false;
			}

			final LazyLongIterator successors = nodeIterator.successors();
			long target;
			while ((target = successors.nextLong()) != -1) {
				final int targetType = (int)nodeTypes.getLong(target);
				succ[targetType].add(id(target));
			}

			final LazyLongIterator typedSuccessors = typedGraph.successors(typedSource);

			for (int type = 0; type < numTypes; type++) {
				for (int i = 0; i < succ[type].size(); i++) {
					final long s = succ[type].getLong(i);
					final int succType = (int)nodeTypes.getLong(s);
					final long succId = BigArrays.get(id, s);
					final long node = node(succType, succId);
					final long typedSucc = typedSuccessors.nextLong();
					if (node != typedSucc) {
						LOGGER.error("Spurious successor: " + node + " (succType " + succType + ", id " + succId + ") instead of " + typedSucc);
						result = false;
					}
				}
				succ[type].clear();
			}

			if (typedSuccessors.nextLong() != -1) {
				LOGGER.error("Spurious successors");
				result = false;
			}

			if (pl != null) pl.update(nodeIterator.outdegree());
		}

		if (pl != null) pl.done();

		return result;
	}

	public static void main(final String[] args) throws JSAPException, ClassNotFoundException, IOException, ConfigurationException {

		final SimpleJSAP jsap = new SimpleJSAP(ConvertToTypedGraph.class.getName(), "Converts an ImmutableGraph to a BVImmutableTypedGraph.", new Parameter[] {
				new Switch("verify", 'v', "verify", "Verifies a conversion, rather than performing it."),
				new UnflaggedOption("sourceGraphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the source graph."),
				new UnflaggedOption("typeGraphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the type graph."),
				new UnflaggedOption("destGraphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the converted graph."),
				new UnflaggedOption("labels", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The serialized LongBigList of node types."), });

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final String sourceGraphBasename = jsapResult.getString("sourceGraphBasename");
		final String typeGraphBasename = jsapResult.getString("typeGraphBasename");
		final String destGraphBasename = jsapResult.getString("destGraphBasename");
		final String labelFile = jsapResult.getString("labels");

		LOGGER.info("Loading graph...");
		final ImmutableGraph graph = ImmutableGraph.loadOffline(sourceGraphBasename);
		LOGGER.info("Loading type graph...");
		final ImmutableGraph typeGraph = ImmutableGraph.wrap(new ArrayListMutableGraph(it.unimi.dsi.webgraph.ImmutableGraph.load(typeGraphBasename)).immutableView());
		LOGGER.info("Loading labels...");
		final LongBigList labels = (LongBigList)BinIO.loadObject(labelFile);

		if (jsapResult.getBoolean("verify")) {
			LOGGER.info("Loading typed graph...");
			final BVImmutableTypedGraph typedGraph = BVImmutableTypedGraph.load(destGraphBasename, typeGraphBasename, null);
			LOGGER.info("Loading ids...");
			final long[][] loadLongsBig = BinIO.loadLongsBig(destGraphBasename + IDS_EXTENSION);
			System.out.println(verify(graph, typeGraph, labels, loadLongsBig, typedGraph, new ProgressLogger(LOGGER)));
		} else {
			LOGGER.info("Converting...");
			convert(graph, typeGraph, labels, destGraphBasename, new ProgressLogger(LOGGER));
		}
	}
}
