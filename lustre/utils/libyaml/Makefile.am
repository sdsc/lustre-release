# libyaml Makefile

SUBDIRS =

install-exec-hook: libyaml.so
	@$(NORMAL_INSTALL)
	$(mkinstalldirs) $(DESTDIR)$(libdir)
	@list=$< ; for p in $$list; do \
	  if test -f $$p; then \
	    f="`echo $$p | sed -e 's|^.*/||'`"; \
	    echo " $(INSTALL_DATA) $$p $(DESTDIR)$(libdir)/$$f"; \
	    $(INSTALL_DATA) $$p $(DESTDIR)$(libdir)/$$f; \
	  else :; fi; \
	done


EXTRA_DIST = yaml.h libyaml.so
