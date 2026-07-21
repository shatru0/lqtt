FROM elixir:1.20

RUN mix local.hex --force && mix local.rebar --force

WORKDIR /app
COPY mix.exs mix.lock ./
RUN mix deps.get --only prod
RUN mix deps.compile

COPY lib/ lib/
RUN mix compile

EXPOSE 1883

CMD ["mix", "run", "--no-halt"]
