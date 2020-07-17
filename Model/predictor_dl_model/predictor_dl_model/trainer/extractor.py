# MIT License
# Copyright (c) 2018 Artur Suilin
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import re
import pandas as pd
import numpy as np

term_pat = re.compile('(.+?):(.+)')
pat = re.compile(
    '(.+)_([a-z][a-z]\.)?((?:wikipedia\.org)|(?:commons\.wikimedia\.org)|(?:www\.mediawiki\.org))_([a-z_-]+?)$')

# Debug output to ensure pattern still works
# print(pat.fullmatch('BLEACH_zh.wikipedia.org_all-accessspider').groups())
# print(pat.fullmatch('Accueil_commons.wikimedia.org_all-access_spider').groups())

def extract(source) -> pd.DataFrame:
    """
    Extracts features from url. Features: agent, site, country, term, marker
    :param source: urls
    :return: DataFrame, one column per feature
    """
    if isinstance(source, pd.Series):
        source = source.values
    agents = np.full_like(source, np.NaN)
    sites = np.full_like(source, np.NaN)
    countries = np.full_like(source, np.NaN)
    terms = np.full_like(source, np.NaN)
    markers = np.full_like(source, np.NaN)

    for i in range(len(source)):
        l = source[i]
        match = pat.fullmatch(l)
        assert match, "Non-matched string %s" % l
        term = match.group(1)
        country = match.group(2)
        if country:
            countries[i] = country[:-1]
        site = match.group(3)
        sites[i] = site
        agents[i] = match.group(4)
        if site != 'wikipedia.org':
            term_match = term_pat.match(term)
            if term_match:
                markers[i] = term_match.group(1)
                term = term_match.group(2)
        terms[i] = term

    return pd.DataFrame({
        'agent': agents,
        'site': sites,
        'country': countries,
        'term': terms,
        'marker': markers,
        'page': source
    })
