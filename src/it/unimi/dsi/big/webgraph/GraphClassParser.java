/*
 * Copyright (C) 2007-2022 Sebastiano Vigna
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

import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.stringparsers.ClassStringParser;

/** A small wrapper around JSAP's standard {@link ClassStringParser}. It
 * tries to prefix the package names in {@link #PACKAGE} to the provided
 * class name, making the specification of graph classes on the command line much easier. */

public class GraphClassParser extends ClassStringParser {
	/** The packages that will be prepended to each graph class. */
	public final static String[] PACKAGE = { "it.unimi.dsi.big.webgraph", "it.unimi.dsi.big.webgraph.labelling" };

	private final static GraphClassParser INSTANCE = new GraphClassParser();

	@SuppressWarnings("deprecation")
	protected GraphClassParser() {}

	public static ClassStringParser getParser() {
		return INSTANCE;
	}

	/** Parses the given class name, but as a first try prepends the package names found in {@link #PACKAGE}.
	 * @param className the name of a class, possibly without package specification.
	 */
	@Override
	public Object parse(final String className) throws ParseException {
		for(final String p: PACKAGE) {
			try {
				return super.parse(p + "." + className);
			}
			catch(final Exception notFound) {}
		}
		return super.parse(className);
	}
}
