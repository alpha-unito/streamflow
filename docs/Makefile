# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

checksum: html
	@echo "Documentation checksum: "
	@find build/html/ 			\
			-not -name 'searchindex.js'	\
			-not -name '*.woff' 		\
			-not -name '*.woff2' 		\
			-type f -print0 	| 	\
		sort -zd 			|	\
		xargs -r0 sha256sum 		|	\
		sha256sum			|	\
		head -c 64
	@echo ""
