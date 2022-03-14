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

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.big.webgraph.AbstractLazyLongIterator;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.GraphClassParser;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;

/** Exhibits an arc-labelled immutable graph as another arc-labelled immutable graph changing only
 *  the kind of labels. Labels of the source graphs are mapped to labels
 *  of the exhibited graph via a suitable strategy provided at construction time.
 */
public class ArcRelabelledImmutableGraph extends ArcLabelledImmutableGraph {

	private static final Logger LOGGER = LoggerFactory.getLogger(ArcRelabelledImmutableGraph.class);

	/** A way to convert a label into another label.
	 */
	public static interface LabelConversionStrategy {
		/** Takes a label <code>from</code> and writes its content into another label <code>to</code>.
		 *  If the types of labels are incompatible, or unapt for this strategy, an {@link IllegalArgumentException}
		 *  or a {@link ClassCastException} will be thrown.
		 *
		 * @param from source label.
		 * @param to target label.
		 * @param source the source node of the arc labelled by the two labels.
		 * @param target the target node of the arc labelled by the two labels.
		 */
		public void convert(Label from, Label to, long source, long target);
	}

	/** A conversion strategy that converts between any two classes extending {@link AbstractIntLabel}.
	 */
	public static final LabelConversionStrategy INT_LABEL_CONVERSION_STRATEGY = (from, to, source, target) -> ((AbstractIntLabel)to).value = ((AbstractIntLabel)from).value;

	/** The wrapped graph. */
	private final ArcLabelledImmutableGraph wrappedGraph;
	/** The new type of labels. */
	private final Label newLabelPrototype;
	/** The conversion strategy to be used. */
	private final LabelConversionStrategy conversionStrategy;

	/** Creates a relabelled graph with given label prototype.
	 *
	 * @param wrappedGraph the graph we are going to relabel.
	 * @param newLabelPrototype the prototype for the new type of labels.
	 * @param conversionStrategy the strategy to convert the labels of the wrapped graph into the new labels.
	 */
	public ArcRelabelledImmutableGraph(final ArcLabelledImmutableGraph wrappedGraph, final Label newLabelPrototype, final LabelConversionStrategy conversionStrategy) {
		this.wrappedGraph = wrappedGraph;
		this.newLabelPrototype = newLabelPrototype;
		this.conversionStrategy = conversionStrategy;
	}

	@Override
	public ArcRelabelledImmutableGraph copy() {
		return new ArcRelabelledImmutableGraph(wrappedGraph.copy(), newLabelPrototype.copy(), conversionStrategy);
	}

	private final class RelabelledArcIterator extends AbstractLazyLongIterator implements LabelledArcIterator {
		/** The wrapped arc iterator. */
		private final LabelledArcIterator wrappedArcIterator;
		/** The source node of the current {@link #wrappedArcIterator}. */
		private final long source;
		/** The target of the current arc. */
		private long target;

		public RelabelledArcIterator(final LabelledArcIterator wrappedArcIterator, final long source) {
			this.wrappedArcIterator = wrappedArcIterator;
			this.source = source;
		}

		@Override
		public Label label() {
			conversionStrategy.convert(wrappedArcIterator.label(), newLabelPrototype, source, target);
			return newLabelPrototype;
		}

		@Override
		public long nextLong() {
			return target = wrappedArcIterator.nextLong();
		}
	}

	@Override
	public ArcLabelledNodeIterator nodeIterator(final long from) {
		return new ArcLabelledNodeIterator() {
			/** The current node. */
			private long current = -1;

			ArcLabelledNodeIterator wrappedNodeIterator = wrappedGraph.nodeIterator(from);
			@Override
			public LabelledArcIterator successors() {
				return new RelabelledArcIterator(wrappedNodeIterator.successors(), current);
			}

			@Override
			public long outdegree() {
				return wrappedNodeIterator.outdegree();
			}

			@Override
			public boolean hasNext() {
				return wrappedNodeIterator.hasNext();
			}

			@Override
			public long nextLong() {
				return current = wrappedNodeIterator.nextLong();
			}

		};
	}

	@Override
	public LabelledArcIterator successors(final long x) {
		return new RelabelledArcIterator(wrappedGraph.successors(x), x);
	}

	@Override
	public Label prototype() {
		return newLabelPrototype;
	}

	@Override
	public long numNodes() {
		return wrappedGraph.numNodes();
	}

	@Override
	public boolean randomAccess() {
		return wrappedGraph.randomAccess();
	}

	@Override
	public long outdegree(final long x) {
		return wrappedGraph.outdegree(x);
	}

	public static void main(final String arg[]) throws JSAPException, IOException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException {
		final SimpleJSAP jsap = new SimpleJSAP(ArcRelabelledImmutableGraph.class.getName(),
				"Relabels a graph with given basename, with integer labels, saving it with a different basename and " +
				"using another (typically: different) type of integer labels, specified via a spec, and possibly using " +
				"a different kind of graph class.",
				new Parameter[] {
						new FlaggedOption("underlyingGraphClass", GraphClassParser.getParser(), BVGraph.class.getName(), JSAP.NOT_REQUIRED, 'u', "underlying-graph-class", "Forces a Java immutable graph class to be used for saving the underlying graph (if the latter did not exist before)."),
						new FlaggedOption("graphClass", GraphClassParser.getParser(), BitStreamArcLabelledImmutableGraph.class.getName(), JSAP.NOT_REQUIRED, 'g', "graph-class", "Forces a Java arc-labelled graph class to be used for saving."),
						new UnflaggedOption("spec", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The label spec (e.g. FixedWidthIntLabel(FOO,10))."),
						new UnflaggedOption("source", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the source arc-labelled graph."),
						new UnflaggedOption("target", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the target arc-labelled graph."),
					}
				);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;
		final Class<?> destClass = jsapResult.getClass("graphClass");
		final Class<?> underlyingDestClass = jsapResult.getClass("underlyingGraphClass");
		final String sourceBasename = jsapResult.getString("source");
		final String targetBasename = jsapResult.getString("target");
		final String spec = jsapResult.getString("spec");
		final Label label = ObjectParser.fromSpec(spec, Label.class);

		final ImmutableGraph source = ImmutableGraph.loadOffline(sourceBasename);
		if (! (source instanceof ArcLabelledImmutableGraph)) throw new IllegalArgumentException("The graph " + sourceBasename + " of class " + sourceBasename.getClass().getName() + " is not arc-labelled");
		final ArcLabelledImmutableGraph labSource = (ArcLabelledImmutableGraph)source;

		if (! (labSource.prototype() instanceof AbstractIntLabel && label instanceof AbstractIntLabel)) throw new IllegalArgumentException("Relabelling from command line is only allowed for int labels, not for " + labSource.prototype().getClass().getName() + " -> " + label.getClass().getName());
		final ArcLabelledImmutableGraph labTarget = new ArcRelabelledImmutableGraph(labSource, label, ArcRelabelledImmutableGraph.INT_LABEL_CONVERSION_STRATEGY);

		final ProgressLogger pl = new ProgressLogger(LOGGER);

		final Properties prop = new Properties();
		prop.load(new FileInputStream(sourceBasename + ImmutableGraph.PROPERTIES_EXTENSION));
		String underlyingBasename = prop.getProperty(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY); // Tries to get the underlying basename
		if (underlyingBasename == null)
			// If the underlying did not exist, we store it with a fixed basename variant
			underlyingDestClass.getMethod("store", ImmutableGraph.class, CharSequence.class, ProgressLogger.class)
			.invoke(null, labTarget, underlyingBasename = targetBasename + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX, pl);

		destClass.getMethod("store", ArcLabelledImmutableGraph.class, CharSequence.class, CharSequence.class, ProgressLogger.class)
			.invoke(null, labTarget, targetBasename, underlyingBasename, pl);

	}


}
