/*
 * Copyright (C) 2007-2022 Paolo Boldi
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
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.Test;

import it.unimi.dsi.big.webgraph.BVGraphTest;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterators;
import it.unimi.dsi.big.webgraph.WebGraphTestCase;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;

public class CSSerializationTest extends WebGraphTestCase {

	private static final int[] SIZES = { 0, 1, 2, 3, 4 };
	private static final int[] WIDTHS = {  20, 21, 30, 31 };

	public String createGraph(final File basename, final ImmutableGraph g, final int width) throws IllegalArgumentException, SecurityException, IOException {
		final int n = (int)g.numNodes();
		System.err.println("Testing " + n + " nodes, width " + width+ ", basename " + basename);

		final OutputBitStream labels = new OutputBitStream(basename + "-fixedlabel" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION);
		final OutputBitStream offsets = new OutputBitStream(basename + "-fixedlabel" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION);
		Label lab;
		offsets.writeGamma(0);
		for(int i = 0; i < n; i++) {
			int bits = 0;
			for(final LongIterator j = LazyLongIterators.eager(g.successors(i)); j.hasNext();) {
				final int succ = (int)j.nextLong();
				lab = new FakeCSFixedWidthIntLabel("TEST", width, i * succ + i);
				bits += lab.toBitStream(labels, i);
			}
			offsets.writeGamma(bits);
		}
		labels.close();
		offsets.close();

		final PrintWriter pw = new PrintWriter(new FileWriter(basename + "-fixedlabel" + ImmutableGraph.PROPERTIES_EXTENSION));
		pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
		pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + FakeCSFixedWidthIntLabel.class.getName() + "(TEST," + width + ")");
		pw.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = " + basename.getName());
		pw.close();

		return basename + "-fixedlabel";
	}

	public void testLabels(final ArcLabelledImmutableGraph alg, final int width) {

		final int mask = (int)((1L << width) - 1);

		// Sequential access, iterators
		for(final ArcLabelledNodeIterator nodeIterator = alg.nodeIterator(); nodeIterator.hasNext();) {
			final int curr = (int)nodeIterator.nextLong();
			final ArcLabelledNodeIterator.LabelledArcIterator l = nodeIterator.successors();
			int d = (int)nodeIterator.outdegree();
			while(d-- != 0) {
				final int succ = (int)l.nextLong();
				assertEquals(curr + " -> " + succ,(curr * succ + curr) & mask, l.label().getInt());
			}
		}

		// Sequential access, arrays
		for(final ArcLabelledNodeIterator nodeIterator = alg.nodeIterator(); nodeIterator.hasNext();) {
			final int curr = (int)nodeIterator.nextLong();
			final int d = (int)nodeIterator.outdegree();
			final long[][] succ = nodeIterator.successorBigArray();
			final Label[][] label = nodeIterator.labelBigArray();
			for(int i = 0; i < d; i++)
				assertEquals(curr + " -> " + get(succ, i), (curr * get(succ, i) + curr) & mask, get(label, i).getInt());
		}

		if (! alg.randomAccess()) return;

		// Random access, iterators
		for(int curr = 0; curr < alg.numNodes(); curr++) {
			final ArcLabelledNodeIterator.LabelledArcIterator l = alg.successors(curr);
			int d = (int)alg.outdegree(curr);
			while(d-- != 0) {
				final int succ = (int)l.nextLong();
				assertEquals(curr + " -> " + succ ,(curr * succ + curr) & mask, l.label().getInt());
			}
		}

		// Random access, arrays
		for(int curr = 0; curr < alg.numNodes(); curr++) {
			final int d = (int)alg.outdegree(curr);
			final long[][] succ = alg.successorBigArray(curr);
			final Label[][] label = alg.labelBigArray(curr);
			for(int i = 0; i < d; i++) {
				assertEquals(curr + " -> " + get(succ, i), (curr * get(succ, i) + curr) & mask, get(label, i).getInt());
			}
		}
	}

	@Test
	public void testLabels() throws IOException, IllegalArgumentException, SecurityException {
		for(final int n: SIZES) {
			for(int type = 0; type < 3; type++) {
				System.err.println("Testing type " + type + "...");
				final ImmutableGraph g = type == 0 ? ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(n, false).immutableView()) :
					type == 1 ? ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteBinaryIntree(n).immutableView()) :
						ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteBinaryOuttree(n).immutableView());
				final File basename = BVGraphTest.storeTempGraph(g);
				for(final int width: WIDTHS) {
					final String basenameLabel = createGraph(basename, g, width);

					System.err.println("Testing offline...");
					testLabels(BitStreamArcLabelledImmutableGraph.loadOffline(basenameLabel), width);
					System.err.println("Testing standard...");
					testLabels(BitStreamArcLabelledImmutableGraph.load(basenameLabel), width);

					new File(basenameLabel + ImmutableGraph.PROPERTIES_EXTENSION).delete();
					new File(basenameLabel + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION).delete();
					new File(basenameLabel + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION).delete();
				}
				basename.delete();
				deleteGraph(basename);
			}
		}
	}
}
