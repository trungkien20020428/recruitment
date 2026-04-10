FROM ruby:3.4-slim

WORKDIR /app

COPY Gemfile ./
RUN bundle install --without development

COPY aggregator.rb ./

RUN useradd -m appuser
USER appuser

ENTRYPOINT ["ruby", "aggregator.rb"]
CMD ["--input", "/data/ad_data.csv", "--output", "/output"]
