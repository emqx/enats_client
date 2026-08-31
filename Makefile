.PHONY: all compile test coverage dialyzer xref format format-check specs edoc static_checks clean

all: test

compile:
	rebar3 compile

test:
	rebar3 ct --suite test/enats_client_SUITE.erl

coverage:
	rebar3 ct --cover --suite test/enats_client_SUITE.erl
	coverage=$$(rebar3 cover --verbose 2>&1 | awk -F'|' '/total/{gsub("%","",$$3); print $$3+0}'); test "$$coverage" -ge 75

dialyzer:
	rebar3 dialyzer

xref:
	rebar3 xref

format:
	rebar3 fmt -w

format-check:
	rebar3 fmt --check

specs:
	./scripts/check_specs.escript

edoc:
	rebar3 edoc

static_checks: compile format-check specs dialyzer xref edoc

clean:
	rebar3 clean
