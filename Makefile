.PHONY: all compile test coverage dialyzer xref clean

all: test

compile:
	rebar3 compile

test:
	rebar3 ct --suite test/enats_client_SUITE.erl

coverage:
	rebar3 ct --cover --suite test/enats_client_SUITE.erl
	coverage=$$(rebar3 cover --verbose 2>&1 | awk -F'|' '/total/{gsub("%","",$$3); print $$3+0}'); test "$$coverage" -gt 80

dialyzer:
	rebar3 dialyzer

xref:
	rebar3 xref

clean:
	rebar3 clean
