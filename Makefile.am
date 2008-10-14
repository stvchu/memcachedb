bin_PROGRAMS = memcachedb
memcachedb_SOURCES = memcachedb.c item.c memcachedb.h thread.c bdb.c stats.c

SUBDIRS = doc tools
EXTRA_DIST = doc tools CREDITS AUTHORS LICENSE

dist-hook:
	rm -rf $(distdir)/doc/.svn/
	rm -rf $(distdir)/tools/.svn/
