/*
 * Copyright (C) 2007-2020 Paolo Boldi
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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.Transform;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;


public class MoreLabelledTransformTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(MoreLabelledTransformTest.class);

	@Test
	public void testTransform() throws IOException, IllegalArgumentException, SecurityException {
		final File f = File.createTempFile("test", "transform");
		f.delete();
		f.mkdir();
		f.deleteOnExit();
		System.out.println(f);
		final ProgressLogger pl = new ProgressLogger(LOGGER);
		pl.logInterval = 1;

		// Creates an arc-labelled graph
		int[][] arcs;
		final ArrayListMutableGraph under = new ArrayListMutableGraph(6, arcs = new int[][] {
				{ 0, 3 }, { 1, 3 }, { 1, 4 }, { 2, 4 }, { 5, 4 }
		});
		BVGraph.store(ImmutableGraph.wrap(under.immutableView()), new File(f, "original" + BitStreamArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX).toString());
		final OutputBitStream obs = new OutputBitStream(new File(f, "original" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION).toString());
		final OutputBitStream labobs = new OutputBitStream(new FileOutputStream(new File(f, "original" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION).toString()));
		long prev = 0;
		int curr = -1;
		for (final int[] arc: arcs) {
			while (arc[0] != curr) {
				labobs.writeGamma((int)(obs.writtenBits() - prev));
				prev = obs.writtenBits();
				curr++;
			}
			new FixedWidthIntLabel("fake", 8, arc[0] * arc[1]).toBitStream(obs, arc[0]);
		}
		labobs.writeGamma((int)(obs.writtenBits() - prev));
		obs.close();
		labobs.close();
		final String graphBasename = new File(f, "original").toString();
		final PrintWriter pw = new PrintWriter(graphBasename + ArcLabelledImmutableGraph.PROPERTIES_EXTENSION);
		pw.println(BitStreamArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + "=original" + BitStreamArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX);
		pw.println(ArcLabelledImmutableGraph.GRAPHCLASS_PROPERTY_KEY + "=" + BitStreamArcLabelledImmutableGraph.class.getName());
		pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + "=" + FixedWidthIntLabel.class.getName() + "(fake,8,0)");
		pw.close();

		// We transpose it
		final ArcLabelledImmutableGraph graph = ArcLabelledImmutableGraph.load(graphBasename, pl);
		ArcLabelledImmutableGraph gT = Transform.transposeOffline(graph, 50, null, null);
		final String baseNameT = graphBasename + "t";
		BVGraph.store(gT, baseNameT + "-underlying");
		BitStreamArcLabelledImmutableGraph.store(gT, baseNameT, baseNameT + "-underlying");

		// We reload the transpose
		gT = ArcLabelledImmutableGraph.load(baseNameT, pl);

		// We merge it with the original one
		final LabelMergeStrategy mergeStrategy = null;
		ArcLabelledImmutableGraph gU = Transform.union(graph, gT, mergeStrategy);
		final String baseNameU = graphBasename + "u";
		BVGraph.store(gU, baseNameU + "-underlying");
		BitStreamArcLabelledImmutableGraph.store(gU, baseNameU, baseNameU + "-underlying");

		// We reload it
		gU = BitStreamArcLabelledImmutableGraph.load(baseNameU, pl);
		System.out.println(gU);

		// Here is what we expect to find
		final int[][] expectedSuccessors = new int[][] {
				{ 3 }, // successors of 0
				{ 3, 4 }, // successors of 1
				{ 4 }, // successors of 2
				{ 0, 1 }, // successors of 3
				{ 1, 2, 5 }, // successors of 4
				{ 4 }, // successors of 5
		};
		final int[][] expectedLabels = new int[][] {
				{ 0 }, // successors of 0
				{ 3, 4 }, // successors of 1
				{ 8 }, // successors of 2
				{ 0, 3 }, // successors of 3
				{ 4, 8, 20 }, // successors of 4
				{ 20 }, // successors of 5
		};
		final ArcLabelledNodeIterator nit = gU.nodeIterator();
		while (nit.hasNext()) {
			final int node = (int)nit.nextLong();
			assertEquals(expectedSuccessors[node].length, nit.outdegree());
			final LabelledArcIterator ait = nit.successors();
			int d = (int)nit.outdegree();
			int k = 0;
			while (d-- != 0) {
				assertEquals(expectedSuccessors[node][k], ait.nextLong());
				assertEquals(expectedLabels[node][k], ait.label().getInt());
				k++;
			}
		}

		// Same test, but with iterators requested randomly
		for (int node = (int)(gU.numNodes() - 1); node >= 0; node--) {
			final LabelledArcIterator ait = gU.successors(node);
			assertEquals(expectedSuccessors[node].length, gU.outdegree(node));
			int k = 0;
			int d = (int)gU.outdegree(node);
			while (d-- != 0) {
				assertEquals(expectedSuccessors[node][k], ait.nextLong());
				assertEquals(expectedLabels[node][k], ait.label().getInt());
				k++;
			}
		}
	}
}
