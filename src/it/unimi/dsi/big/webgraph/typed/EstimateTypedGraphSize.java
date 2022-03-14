/*
 * Copyright (C) 2020-2022 Paolo Boldi and Sebastiano Vigna
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

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
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
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.io.NullOutputStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.stat.SummaryStats;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;

public class EstimateTypedGraphSize {
	private static final Logger LOGGER = LoggerFactory.getLogger(EstimateTypedGraphSize.class);

	public static Map<String, SummaryStats> estimate(final ImmutableGraph graph, final it.unimi.dsi.webgraph.ImmutableGraph typeGraph, final LongBigList nodeTypes, final ProgressLogger pl) throws IOException {
		final Object2ObjectOpenHashMap<String, SummaryStats> data = new Object2ObjectOpenHashMap<>();
		@SuppressWarnings("resource")
		final OutputBitStream countStream = new OutputBitStream(NullOutputStream.getInstance());

		final long[][] id = LongBigArrays.newBigArray(graph.numNodes());
		final long[] count = new long[typeGraph.numNodes()];

		final SummaryStats bitsForOutdegrees = new SummaryStats(), bitsForDeltas = new SummaryStats(),
				emptyLists = new SummaryStats();
		data.put("bitsForOutdegrees", bitsForOutdegrees);
		data.put("bitsForDeltas", bitsForDeltas);
		data.put("emptyLists", emptyLists);

		for (long node = 0; node < graph.numNodes(); node++) {
			BigArrays.set(id, node, count[(int)nodeTypes.getLong(node)]++);
		}

		final LongArrayList[] succ = new LongArrayList[typeGraph.numNodes()];
		for (int i = 0; i < succ.length; i++) succ[i] = new LongArrayList();

		if (pl != null) {
			pl.itemsName = "arcs";
			pl.expectedUpdates = graph.numArcs();
			pl.start("Analyzing...");
		}


		for (final NodeIterator nodeIterator = graph.nodeIterator(); nodeIterator.hasNext();) {
			final long source = nodeIterator.nextLong();
			if ((source & 0x7FFFFFF) == 0) LOGGER.info(data.toString());
			final int type = (int)nodeTypes.getLong(source);
			final LazyLongIterator successors = nodeIterator.successors();
			long target;
			while ((target = successors.nextLong()) != -1) {
				succ[(int)nodeTypes.getLong(target)].add(target);
			}

			final it.unimi.dsi.webgraph.LazyIntIterator successorTypes = typeGraph.successors(type);
			int succType;
			while ((succType = successorTypes.nextInt()) != -1) {
				bitsForOutdegrees.add(countStream.writeGamma(succ[succType].size()));
				long prev = -1;
				if (succ[succType].isEmpty()) emptyLists.add(1);
				for (final long node : succ[succType]) {
					final long t = BigArrays.get(id, node);
					bitsForDeltas.add(countStream.writeLongGamma(t - prev - 1));
					prev = t;
				}

				succ[succType].clear();
			}

			for (final LongArrayList l : succ) {
				if (!l.isEmpty()) {
					LOGGER.error("Detected arc " + source + " -> [" + l.getLong(0) + "] from type " + type + " to type " + nodeTypes.getLong(l.getLong(0)));
					l.clear();
				}
			}

			if (pl != null) pl.update(nodeIterator.outdegree());
		}

		if (pl != null) pl.done();

		return data;
	}

	public static void main(final String[] args) throws JSAPException, ClassNotFoundException, IOException {

		final SimpleJSAP jsap = new SimpleJSAP(BVGraph.class.getName(), "Compresses differentially a graph. Source and destination are basenames from which suitable filenames will be stemmed; alternatively, if the suitable option was specified, source is a spec (see below). For more information about the compression techniques, see the Javadoc documentation.", new Parameter[] {
				new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the source graph."),
				new UnflaggedOption("typeGraphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the type graph."),
				new UnflaggedOption("labels", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The serialized LongBigList of node types."), });

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final String graphBasename = jsapResult.getString("graphBasename");
		final String typeGraphBasename = jsapResult.getString("typeGraphBasename");
		final String labelFile = jsapResult.getString("labels");

		LOGGER.info("Loading graph...");
		final ImmutableGraph graph = ImmutableGraph.loadOffline(graphBasename);
		LOGGER.info("Loading type graph...");
		final it.unimi.dsi.webgraph.ImmutableGraph typeGraph = new ArrayListMutableGraph(it.unimi.dsi.webgraph.ImmutableGraph.load(typeGraphBasename)).immutableView();
		LOGGER.info("Loading labels...");
		final LongBigList labels = (LongBigList)BinIO.loadObject(labelFile);
		LOGGER.info("Running analysis...");

		System.out.println(estimate(graph, typeGraph, labels, new ProgressLogger(LOGGER)));
	}
}
