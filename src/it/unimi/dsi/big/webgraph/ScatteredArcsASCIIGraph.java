/*
 * Copyright (C) 2011-2022 Sebastiano Vigna
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

import static it.unimi.dsi.fastutil.HashCommon.bigArraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;


/**
 * An {@link ImmutableGraph} that corresponds to a graph stored as a scattered list of arcs.
 *
 * <p>
 * A <em>scattered list of arcs</em> describes a graph in a fairly loose way. Each line contains an
 * arc specified as two node identifiers separated by whitespace (but we suggest exactly one TAB
 * character). Sources and targets can be in any order.
 *
 * <p>
 * In the <em>standard</em> description, node identifiers can be in the range
 * [-2<sup>63</sup>..2<sup>63</sup>): they will be remapped in a compact identifier space by
 * assigning to each newly seen identifier a new node number. The list of identifiers in order of
 * appearance is available in {@link #ids}. Lines can be empty, or comments starting with
 * <code>#</code>. Characters following the target will be discarded with a warning.
 *
 * <p>
 * <strong>Warning:</strong> Lines not conforming the above specification will cause an error to be
 * logged, but will be otherwise ignored.
 *
 * <p>
 * Alternatively, you can
 * {@linkplain #ScatteredArcsASCIIGraph(InputStream, Object2LongFunction, Charset, long, boolean, boolean, int, File, ProgressLogger)
 * provide} an {@link Object2LongFunction Object2LongFunction&lt;String>} with default return value
 * -1 that will be used to map identifiers to node numbers, along with a {@link Charset} to parse
 * lines and the number of nodes of the graph (which must be a strict upper bound for the largest
 * value returned by the function).
 *
 * <p>
 * A faster conversion can be obtained by
 * {@linkplain #ScatteredArcsASCIIGraph(InputStream, Object2LongFunction, long, boolean, boolean, int, File, ProgressLogger)
 * providing} an {@link Object2LongFunction Object2LongFunction&lt;byte[]>}: in this case, no
 * character decoding is performed, and keys are bytes sequences. This technique goes hand in hand
 * with classes such as {@link GOV3Function}, which can store efficiently a map from byte arrays to
 * longs.
 *
 * <p>
 * Additionally, the resulting graph can be symmetrized, and its loops be removed, using
 * {@linkplain #ScatteredArcsASCIIGraph(InputStream, boolean, boolean, int, File, ProgressLogger)
 * suitable constructor options}.
 *
 * <P>
 * This class has no load method, and its main method converts a scattered-arcs representation
 * directly into a {@link BVGraph}.
 *
 * <h2>Using {@link ScatteredArcsASCIIGraph} to convert your data</h2>
 *
 * <p>
 * A simple (albeit rather inefficient) way to import data into WebGraph is using ASCII graphs
 * specified by scattered arcs. Suppose you create the following file, named
 * <code>example.arcs</code>:
 *
 * <pre>
 *  # My graph
 *  -1 15
 *  15 2
 *  2 -1 This will cause a warning to be logged
 *  OOPS! (This will cause an error to be logged)
 *  -1 2
 * </pre>
 *
 * Then, the command
 *
 * <pre>
 *  java it.unimi.dsi.big.webgraph.ScatteredArcsASCIIGraph example &lt;example.arcs
 * </pre>
 *
 * will produce a compressed graph in {@link it.unimi.dsi.webgraph.BVGraph} format with basename
 * <code>example</code>. The file <code>example.ids</code> will contain the list of longs -1, 15, 2.
 * The node with identifer -1 will be the node 0 in the output graph, the node with identifier 15
 * will be node 1, and the node with identifier 2 will be node 2. The graph <code>example</code>
 * will thus have three nodes and four arcs (viz., &lt;0,1&gt;, &lt;0,2&gt;, &lt;1,2&gt; and
 * &lt;2,0&gt;).
 *
 * <h2>Memory requirements</h2>
 *
 * <p>
 * To convert node identifiers to node numbers, instances of this class use a custom map that in the
 * worst case will require
 * 25.5&times;2<sup>&lceil;log(4<var>n</var>/3)&rceil;</sup>&nbsp;&le;&nbsp;68<var>n</var> bytes,
 * where <var>n</var> is the number of distinct identifiers. Storing batches of arcs in memory
 * requires 16 bytes per arc.
 */


public class ScatteredArcsASCIIGraph extends ImmutableSequentialGraph {
	private static final Logger LOGGER = LoggerFactory.getLogger(ScatteredArcsASCIIGraph.class);
	private final static boolean DEBUG = false;

	/** The default batch size. */
	public static final int DEFAULT_BATCH_SIZE = 10000000;
	/** The extension of the identifier file (a binary list of longs). */
	private static final String IDS_EXTENSION = ".ids";
	/** The batch graph used to return node iterators. */
	private final Transform.BatchGraph batchGraph;
	/** The big-array list of identifiers in order of appearance. */
	public long[][] ids;

	public static class Id2NodeMap implements Hash {
		/** The big array of keys. */
		protected long[][] key;

		/** The big array of values. */
		protected long[][] value;

		/** Whether the zero key is present (the value is stored in {@link #zeroValue). */
		protected boolean containsZeroKey;

		/** The value associated with the zero key, if {@link #containsZeroKey}. */
		protected long zeroValue;

		/** The acceptable load factor. */
		protected final float f;

		/** The current table size (always a power of 2). */
		protected long n;

		/** Threshold after which we rehash. It must be the table size times {@link #f}. */
		protected long maxFill;

		/** The mask for wrapping a position counter. */
		protected long mask;

		/** The mask for wrapping a segment counter. */
		protected int segmentMask;

		/** The mask for wrapping a base counter. */
		protected int baseMask;

		/** Number of entries in the set. */
		protected long size;

		private void initMasks() {
			mask = n - 1;
			baseMask = key.length - 1;
			/* Note that either we have more than one segment, and in this case all segments
			 * are BigArrays.SEGMENT_SIZE long, or we have exactly one segment whose length
			 * is a power of two. */
			segmentMask = key[0].length - 1;
		}

		/**
		 * Creates a new map based on a hash table.
		 *
		 * <p>
		 * The actual table size will be the least power of two greater than
		 * <code>expected</code>/<code>f</code>.
		 *
		 * @param expected the expected number of elements in the map.
		 * @param f the load factor.
		 */
		public Id2NodeMap(final long expected, final float f) {
			if (f <= 0 || f > 1) throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
			if (n < 0) throw new IllegalArgumentException("The expected number of elements must be nonnegative");
			this.f = f;
			n = bigArraySize(expected, f);
			maxFill = maxFill(n, f);
			key = LongBigArrays.newBigArray(n);
			value = LongBigArrays.newBigArray(n);
			initMasks();
		}

		/**
		 * Creates a new hash big set with initial expected {@link Hash#DEFAULT_INITIAL_SIZE} elements
		 * and {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
		 */

		public Id2NodeMap() {
			this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
		}

		/**
		 * Returns the node associated with a given identifier, assigning a new one if necessary.
		 *
		 * @param id an identifier.
		 * @return the associated node.
		 */
		public long getNode(final long id) {
			if (id == 0) {
				if (containsZeroKey) return zeroValue;
				zeroValue = size;
				containsZeroKey = true;
			} else {

				final long h = it.unimi.dsi.fastutil.HashCommon.mix(id);

				// The starting point.
				int displ = (int)(h & segmentMask);
				int base = (int)((h & mask) >>> BigArrays.SEGMENT_SHIFT);

				// There's always an unused entry.
				while (key[base][displ] != 0) {
					if (id == key[base][displ]) return value[base][displ];
					base = (base + ((displ = (displ + 1) & segmentMask) == 0 ? 1 : 0)) & baseMask;
				}

				key[base][displ] = id;
				value[base][displ] = size;
			}

			if (++size >= maxFill) rehash(2 * n);
			return size - 1;
		}


		protected void rehash(final long newN) {
			final long key[][] = this.key;
			final long[][] value = this.value;
			final long newKey[][] = LongBigArrays.newBigArray(newN);
			final long newValue[][] = LongBigArrays.newBigArray(newN);
			final long newMask = newN - 1;
			final int newBaseMask = newKey.length - 1;
			final int newSegmentMask = newKey[0].length - 1;
			final long realSize = containsZeroKey ? size - 1 : size;

			int base = 0, displ = 0;

			for (long i = realSize; i-- != 0;) {
				while (key[base][displ] == 0)
					base = (base + ((displ = (displ + 1) & segmentMask) == 0 ? 1 : 0));

				final long k = key[base][displ];
				final long h = it.unimi.dsi.fastutil.HashCommon.mix(k);

				// The starting point.
				int d = (int)(h & newSegmentMask);
				int b = (int)((h & newMask) >>> BigArrays.SEGMENT_SHIFT);

				while (newKey[b][d] != 0)
					b = (b + ((d = (d + 1) & newSegmentMask) == 0 ? 1 : 0)) & newBaseMask;

				newKey[b][d] = k;
				newValue[b][d] = value[base][displ];

				base = (base + ((displ = (displ + 1) & segmentMask) == 0 ? 1 : 0));
			}

			this.n = newN;
			this.key = newKey;
			this.value = newValue;
			initMasks();
			maxFill = maxFill(n, f);
		}

		/**
		 * Returns the id list in order of appearance as a {@linkplain LongBigArrays big array}.
		 *
		 * <p>
		 * The map is not usable after this call.
		 *
		 * @param tempDir a temporary directory for storing keys and values.
		 * @return the id list in order of appearance.
		 */
		public long[][] getIds(final File tempDir) throws IOException {
			// Here we assume that the map is a minimal perfect hash
			final long realSize = containsZeroKey ? size - 1 : size;
			int base = 0, displ = 0, b = 0, d = 0;
			for (long i = realSize; i-- != 0;) {
				while (key[base][displ] == 0) base = (base + ((displ = (displ + 1) & segmentMask) == 0 ? 1 : 0)) & baseMask;
				key[b][d] = key[base][displ];
				value[b][d] = value[base][displ];
				base = (base + ((displ = (displ + 1) & segmentMask) == 0 ? 1 : 0)) & baseMask;
				b = (b + ((d = (d + 1) & segmentMask) == 0 ? 1 : 0)) & baseMask;
			}

			if (containsZeroKey) {
				key[b][d] = 0;
				value[b][d] = zeroValue;
			}

			// The following weird code minimizes memory usage
			final File keyFile = File.createTempFile(ScatteredArcsASCIIGraph.class.getSimpleName(), "keys", tempDir);
			keyFile.deleteOnExit();
			final File valueFile = File.createTempFile(ScatteredArcsASCIIGraph.class.getSimpleName(), "values", tempDir);
			valueFile.deleteOnExit();

			BinIO.storeLongs(key, 0, size(), keyFile);
			BinIO.storeLongs(value, 0, size(), valueFile);

			key = null;
			value = null;

			final long[][] key = BinIO.loadLongsBig(keyFile);
			keyFile.delete();
			final long[][] value = BinIO.loadLongsBig(valueFile);
			valueFile.delete();

			final long[][] result = LongBigArrays.newBigArray(size());
			for (long i = size(); i-- != 0;) BigArrays.set(result, BigArrays.get(value, i), BigArrays.get(key, i));
			return result;
		}

		public long size() {
			return size;
		}
	}


	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is) throws IOException {
		this(is, false);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final boolean symmetrize) throws IOException {
		this(is, symmetrize, false);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final boolean symmetrize, final boolean noLoops) throws IOException {
		this(is, symmetrize, noLoops, DEFAULT_BATCH_SIZE);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize) throws IOException {
		this(is, symmetrize, noLoops, batchSize, null);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir) throws IOException {
		this(is, symmetrize, noLoops, batchSize, tempDir, null);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		this(is, null, null, -1, symmetrize, noLoops, batchSize, tempDir, pl);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final long n) throws IOException {
		this(is, function, charset, n, false);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final long n, final boolean symmetrize) throws IOException {
		this(is, function, charset, n, symmetrize, false);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final long n, final boolean symmetrize, final boolean noLoops) throws IOException {
		this(is, function, charset, n, symmetrize, noLoops, DEFAULT_BATCH_SIZE);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final long n, final boolean symmetrize, final boolean noLoops, final int batchSize) throws IOException {
		this(is, function, charset, n, symmetrize, noLoops, batchSize, null);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final long n, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir) throws IOException {
		this(is, function, charset, n, symmetrize, noLoops, batchSize, tempDir, null);
	}

	/** Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, Charset charset, final long n, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		@SuppressWarnings("resource")
		final FastBufferedInputStream fbis = new FastBufferedInputStream(is);
		final Id2NodeMap map = new Id2NodeMap();

		long numNodes = -1;
		if (charset == null) charset = Charset.forName("ISO-8859-1");

		int j;
		long[] source = new long[batchSize], target = new long[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		if (pl != null) {
			pl.itemsName = "arcs";
			pl.start("Creating sorted batches...");
		}

		j = 0;
		long pairs = 0; // Number of pairs
		byte[] array = new byte[1024];
		for(long line = 1; ; line++) {
			int start = 0, len;
			while((len = fbis.readLine(array, start, array.length - start, FastBufferedInputStream.ALL_TERMINATORS)) == array.length - start) {
				start += len;
				array = ByteArrays.grow(array, array.length + 1);
			}

			if (len == -1) break; // EOF

			final int lineLength = start + len;

			if (DEBUG) System.err.println("Reading line " + line + "... (" + new String(array, 0, lineLength, charset) + ")");

			// Skip whitespace at the start of the line.
			int offset = 0;
			while(offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset == lineLength) {
				if (DEBUG) System.err.println("Skipping line " + line + "...");
				continue; // Whitespace line
			}

			if (array[0] == '#') continue;

			// Scan source id.
			start = offset;
			while(offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			long s;

			if (function == null) {
				final long sl;
				try {
					sl = getLong(array, start, offset - start);
				}
				catch(final RuntimeException e) {
					// Discard up to the end of line
					LOGGER.error("Error at line " + line + ": " + e.getMessage());
					continue;
				}

				s = map.getNode(sl);

				if (DEBUG) System.err.println("Parsed source at line " + line + ": " + sl + " => " + s);
			}
			else {
				final String ss = new String(array, start, offset - start, charset);
				s = function.getLong(ss);
				if (s == -1) {
					LOGGER.warn("Unknown source identifier " + ss + " at line " + line);
					continue;
				}
				if (s < 0 || s >= n) throw new IllegalArgumentException("Source node number out of range for node " + ss + ": " + s);
				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + ss + " => " + s);
			}

			if (DEBUG) System.err.println("Parsed source at line " + line + ": " + s + " => " + s);

			// Skip whitespace between identifiers.
			while(offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset == lineLength) {
				LOGGER.error("Error at line " + line + ": no target");
				continue;
			}

			// Scan target id.
			start = offset;
			while(offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			long t;

			if (function == null) {
				final long tl;
				try {
					tl = getLong(array, start, offset - start);
				}
				catch(final RuntimeException e) {
					// Discard up to the end of line
					LOGGER.error("Error at line " + line + ": " + e.getMessage());
					continue;
				}

				t = map.getNode(tl);

				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + tl + " => " + t);
			}
			else {
				final String ts = new String(array, start, offset - start, charset);
				t = function.getLong(ts);
				if (t == -1) {
					LOGGER.warn("Unknown target identifier " + ts + " at line " + line);
					continue;
				}

				if (t < 0 || t >= n) throw new IllegalArgumentException("Target node number out of range for node " + ts + ": " + t);
				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + ts + " => " + t);
			}

			// Skip whitespace after target.
			while(offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset < lineLength) LOGGER.warn("Trailing characters ignored at line " + line);

			if (s != t || ! noLoops) {
				source[j] = s;
				target[j++] = t;

				if (j == batchSize) {
					pairs += Transform.processBatch(batchSize, source, target, tempDir, batches);
					j = 0;
				}

				if (symmetrize && s != t) {
					source[j] = t;
					target[j++] = s;
					if (j == batchSize) {
						pairs += Transform.processBatch(batchSize, source, target, tempDir, batches);
						j = 0;
					}
				}

				if (pl != null) pl.lightUpdate();
			}
		}

		if (j != 0) pairs += Transform.processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			Transform.logBatches(batches, pairs, pl);
		}

		numNodes = map.size();
		source = null;
		target = null;

		if (function == null) ids = map.getIds(tempDir);
		batchGraph = new Transform.BatchGraph(function == null ? numNodes : n, pairs, batches);
	}


	/**
	 * Creates a scattered-arcs ASCII graph using a function on byte arrays.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from byte arrays representing nodes to node
	 *            numbers.
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not
	 *            <code>null</code>).
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<byte[]> function, final long n) throws IOException {
		this(is, function, n, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph using a function on byte arrays.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from byte arrays representing nodes to node
	 *            numbers.
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not
	 *            <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<byte[]> function, final long n, final boolean symmetrize) throws IOException {
		this(is, function, n, symmetrize, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from byte arrays representing nodes to node
	 *            numbers.
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not
	 *            <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<byte[]> function, final long n, final boolean symmetrize, final boolean noLoops) throws IOException {
		this(is, function, n, symmetrize, noLoops, DEFAULT_BATCH_SIZE);
	}

	/**
	 * Creates a scattered-arcs ASCII graph using a function on byte arrays.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from byte arrays representing nodes to node
	 *            numbers.
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not
	 *            <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<byte[]> function, final long n, final boolean symmetrize, final boolean noLoops, final int batchSize) throws IOException {
		this(is, function, n, symmetrize, noLoops, batchSize, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph using a function on byte arrays.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from byte arrays representing nodes to node
	 *            numbers.
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not
	 *            <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<byte[]> function, final long n, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir) throws IOException {
		this(is, function, n, symmetrize, noLoops, batchSize, tempDir, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph using a function on byte arrays.
	 *
	 * @param is an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from byte arrays representing nodes to node
	 *            numbers.
	 * @param n the number of nodes of the graph (used only if <code>function</code> is not
	 *            <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
	 *            allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for
	 *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public ScatteredArcsASCIIGraph(final InputStream is, final Object2LongFunction<byte[]> function, final long n, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		Objects.requireNonNull(function);
		@SuppressWarnings("resource")
		final FastBufferedInputStream fbis = new FastBufferedInputStream(is);

		int j;
		long[] source = new long[batchSize], target = new long[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		if (pl != null) {
			pl.itemsName = "arcs";
			pl.start("Creating sorted batches...");
		}

		j = 0;
		long pairs = 0; // Number of pairs
		byte[] array = new byte[1024];
		for (long line = 1;; line++) {
			int start = 0, len;
			while ((len = fbis.readLine(array, start, array.length - start, FastBufferedInputStream.ALL_TERMINATORS)) == array.length - start) {
				start += len;
				array = ByteArrays.grow(array, array.length + 1);
			}

			if (len == -1) break; // EOF

			final int lineLength = start + len;

			if (DEBUG) System.err.println("Reading line " + line + "... (" + new String(array, 0, lineLength, Charsets.UTF_8) + ")");

			// Skip whitespace at the start of the line.
			int offset = 0;
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset == lineLength) {
				if (DEBUG) System.err.println("Skipping line " + line + "...");
				continue; // Whitespace line
			}

			if (array[0] == '#') continue;

			// Scan source id.
			start = offset;
			while (offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			long s;

			s = function.getLong(Arrays.copyOfRange(array, start, offset));
			if (s == -1) {
				LOGGER.warn("Unknown source identifier " + new String(array, start, offset - start, Charsets.UTF_8) + " at line " + line);
				continue;
			}
			if (s < 0 || s >= n) throw new IllegalArgumentException("Source node number out of range for node " + new String(array, start, offset - start, Charsets.UTF_8) + ": " + s);

			// Skip whitespace between identifiers.
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset == lineLength) {
				LOGGER.error("Error at line " + line + ": no target");
				continue;
			}

			// Scan target id.
			start = offset;
			while (offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			long t;

			t = function.getLong(Arrays.copyOfRange(array, start, offset));
			if (t == -1) {
				LOGGER.warn("Unknown target identifier " + new String(array, start, offset - start, Charsets.UTF_8) + " at line " + line);
				continue;
			}

			if (t < 0 || t >= n) throw new IllegalArgumentException("Target node number out of range for node " + new String(array, start, offset - start, Charsets.UTF_8) + ": " + t);

			// Skip whitespace after target.
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset < lineLength) LOGGER.warn("Trailing characters ignored at line " + line);

			if (s != t || !noLoops) {
				source[j] = s;
				target[j++] = t;

				if (DEBUG) System.err.println("Adding " + s + " -> " + t);

				if (j == batchSize) {
					pairs += Transform.processBatch(batchSize, source, target, tempDir, batches);
					j = 0;
				}

				if (symmetrize && s != t) {
					source[j] = t;
					target[j++] = s;
					if (j == batchSize) {
						pairs += Transform.processBatch(batchSize, source, target, tempDir, batches);
						j = 0;
					}
				}

				if (pl != null) pl.lightUpdate();
			}
		}

		if (j != 0) pairs += Transform.processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			Transform.logBatches(batches, pairs, pl);
		}

		source = null;
		target = null;

		batchGraph = new Transform.BatchGraph(n, pairs, batches);
	}

	private final static long getLong(final byte[] array, int offset, int length) {
		if (length == 0) throw new NumberFormatException("Empty number");
		int sign = 1;
		if(array[offset] == '-') {
			sign = -1;
			offset++;
			length--;
		}

		long value = 0;
		for(int i = 0; i < length; i++) {
			final byte digit = array[offset + i];
			if (digit < '0' || digit > '9') throw new NumberFormatException("Not a digit: " + (char)digit);
			value *= 10;
			value += digit - '0';
		}

		return sign * value;
	}

	@Override
	public long numNodes() {
		if (batchGraph == null) throw new UnsupportedOperationException("The number of nodes is unknown (you need to exhaust the input)");
		return batchGraph.numNodes();
	}

	@Override
	public long numArcs() {
		if (batchGraph == null) throw new UnsupportedOperationException("The number of arcs is unknown (you need to exhaust the input)");
		return batchGraph.numArcs();
	}

	@Override
	public NodeIterator nodeIterator(final long from) {
		return batchGraph.nodeIterator(from);
	}

	@Override
	public boolean hasCopiableIterators() {
		return batchGraph.hasCopiableIterators();
	}

	@Override
	public ScatteredArcsASCIIGraph copy() {
		return this;
	}

	@SuppressWarnings("unchecked")
	public static void main(final String args[]) throws IllegalArgumentException, SecurityException, IOException, JSAPException, ClassNotFoundException {
		String basename;
		final SimpleJSAP jsap = new SimpleJSAP(ScatteredArcsASCIIGraph.class.getName(), "Converts a scattered list of arcs from standard input into a BVGraph. The list of" +
				"identifiers in order of appearance will be saved with extension \"" + IDS_EXTENSION + "\", unless a translation function has been specified.",
				new Parameter[] {
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new FlaggedOption("batchSize", JSAP.INTSIZE_PARSER, Integer.toString(DEFAULT_BATCH_SIZE), JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in arcs."),
						new FlaggedOption("tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "temp-dir", "A directory for all temporary batch files."),
						new Switch("symmetrize", 'S', "symmetrize", "Force the output graph to be symmetric."),
						new Switch("noLoops", 'L', "no-loops", "Remove loops from the output graph."),
						new Switch("zipped", 'z', "zipped", "The string list is compressed in gzip format."),
						new FlaggedOption("function", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "function", "A serialised function from strings to longs that will be used to translate identifiers to node numbers."),
						new Switch("byteArray", 'b', "byte-array", "The function accepts byte arrays (no character encoding)."),
						new FlaggedOption("charset", JSAP.STRING_PARSER, "ISO-8859-1", JSAP.NOT_REQUIRED, 'C', "charset", "The charset used to read the list of arcs."),
						new FlaggedOption("n", JSAP.LONGSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'n', "n", "The number of nodes of the graph (only if you specified a function)."),
						new FlaggedOption("comp", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'c', "comp", "A compression flag (may be specified several times).").setAllowMultipleDeclarations(true),
						new FlaggedOption("windowSize", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_WINDOW_SIZE), JSAP.NOT_REQUIRED, 'w', "window-size", "Reference window size (0 to disable)."),
						new FlaggedOption("maxRefCount", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_MAX_REF_COUNT), JSAP.NOT_REQUIRED, 'm', "max-ref-count", "Maximum number of backward references (-1 for ∞)."),
						new FlaggedOption("minIntervalLength", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_MIN_INTERVAL_LENGTH), JSAP.NOT_REQUIRED, 'i', "min-interval-length", "Minimum length of an interval (0 to disable)."),
						new FlaggedOption("zetaK", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_ZETA_K), JSAP.NOT_REQUIRED, 'k', "zeta-k", "The k parameter for zeta-k codes."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the output graph"),
					}
				);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean byteArray = jsapResult.userSpecified("byteArray");
		basename = jsapResult.getString("basename");

		if (byteArray && jsapResult.userSpecified("charset")) throw new IllegalArgumentException("When using byte arrays you cannot specify a charset");

		int flags = 0;
		for(final String compressionFlag: jsapResult.getStringArray("comp")) {
			try {
				flags |= BVGraph.class.getField(compressionFlag).getInt(BVGraph.class);
			}
			catch (final Exception notFound) {
				throw new JSAPException("Compression method " + compressionFlag + " unknown.");
			}
		}

		final int windowSize = jsapResult.getInt("windowSize");
		final int zetaK = jsapResult.getInt("zetaK");
		int maxRefCount = jsapResult.getInt("maxRefCount");
		if (maxRefCount == -1) maxRefCount = Integer.MAX_VALUE;
		final int minIntervalLength = jsapResult.getInt("minIntervalLength");

		if (byteArray) {
			long n = -1;
			final Object2LongFunction<byte[]> function = (Object2LongFunction<byte[]>)BinIO.loadObject(jsapResult.getString("function"));
			n = function instanceof Size64 ? ((Size64)function).size64() : function.size();
			if (n == -1) {
				if (!jsapResult.userSpecified("n")) throw new IllegalArgumentException("You must specify a graph size if you specify a translation function that does not return the size of the key set.");
				n = jsapResult.getLong("n");
			}

			File tempDir = null;
			if (jsapResult.userSpecified("tempDir")) tempDir = new File(jsapResult.getString("tempDir"));

			final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
			final boolean zipped = jsapResult.getBoolean("zipped");
			final InputStream inStream = (zipped ? new GZIPInputStream(System.in) : System.in);
			final ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(inStream, function, n, jsapResult.userSpecified("symmetrize"), jsapResult.userSpecified("noLoops"), jsapResult.getInt("batchSize"), tempDir, pl);
			BVGraph.store(graph, basename, windowSize, maxRefCount, minIntervalLength, zetaK, flags, pl);
		} else {
			Object2LongFunction<String> function = null;
			Charset charset = null;
			long n = -1;
			if (jsapResult.userSpecified("function")) {
				function = (Object2LongFunction<String>)BinIO.loadObject(jsapResult.getString("function"));
				charset = Charset.forName(jsapResult.getString("charset"));
				n = function instanceof Size64 ? ((Size64)function).size64() : function.size();
				if (n == -1) {
					if (!jsapResult.userSpecified("n")) throw new IllegalArgumentException("You must specify a graph size if you specify a translation function that does not return the size of the key set.");
					n = jsapResult.getLong("n");
				}
			}

			File tempDir = null;
			if (jsapResult.userSpecified("tempDir")) tempDir = new File(jsapResult.getString("tempDir"));

			final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
			final boolean zipped = jsapResult.getBoolean("zipped");
			final InputStream inStream = (zipped ? new GZIPInputStream(System.in) : System.in);
			final ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(inStream, function, charset, n, jsapResult.userSpecified("symmetrize"), jsapResult.userSpecified("noLoops"), jsapResult.getInt("batchSize"), tempDir, pl);
			BVGraph.store(graph, basename, windowSize, maxRefCount, minIntervalLength, zetaK, flags, pl);
			if (function == null) BinIO.storeLongs(graph.ids, basename + IDS_EXTENSION);
		}
	}
}
