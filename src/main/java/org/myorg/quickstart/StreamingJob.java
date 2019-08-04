package org.myorg.quickstart;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.apache.http.HttpHost;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class StreamingJob {
	public static void main(String[] args) throws Exception {
		// Configuration
		final String PilotCSVFilePath = "/home/jawad/Desktop/pilots.csv"; // path for pilot csv
		final String ElasticSearchHostName = "127.0.0.1";
		final int ElasticSearchHostPort = 9200;
 		final String ElasticSearchIndexName = "films";
 		final String ElasticSearchIndexType = "pilot-films";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> stream = env.addSource(new SwapiSource());

		DataStream<StarShip> starShipDataStream = stream
				.map(new ParseStarShips())
				.filter(new FilterPilots());

		DataStream<Tuple2<String, String>> pilotUrlDataStream = starShipDataStream
					.flatMap(new FlatPilotUrls())
					.filter(new FilterDuplicates());

		DataStream<Pilot> pilotDataStream = AsyncDataStream.orderedWait(pilotUrlDataStream,
				new hitUrlsAsync(),1000, TimeUnit.SECONDS, 5 )
				.map(new ParsePilots());

		DataStream<Tuple2<String, String>> filmUrlDataStream = pilotDataStream
				.filter(new FilterFilms())
				.flatMap(new FlatFilmUrls());

		DataStream<Film> filmDataStream = AsyncDataStream.orderedWait(filmUrlDataStream,
				new hitUrlsAsync(),1000, TimeUnit.SECONDS, 5 )
				.map(new ParseFilms());

		DataStream<Tuple6<String, String, String, String, String, String>> pilotTupleStream  =  pilotDataStream
				.map(new MapFunction<Pilot, Tuple6<String, String, String, String, String, String>>() {
					@Override
					public Tuple6<String, String, String, String, String, String> map(Pilot pilot) throws Exception {
						return new Tuple6<>(pilot.shipName, pilot.gender, pilot.hairColor, pilot.height, pilot.mass,
								pilot.name);
					}
				});

		pilotTupleStream.writeAsCsv(PilotCSVFilePath, FileSystem.WriteMode.OVERWRITE);

		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost(ElasticSearchHostName, ElasticSearchHostPort, "http"));

		ElasticsearchSink.Builder<Film> esSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<Film>() {
					public IndexRequest createIndexRequest(Film film) {
						Map<String, String> json = new HashMap<>();
						json.put("pilot", film.pilotName);
						json.put("film", film.name);
						json.put("opening_crawl", film.openingCrawl);
						json.put("director", film.director);
						json.put("producer", film.producer);
						json.put("release_date", film.releaseDate);

						return Requests.indexRequest()
								.index(ElasticSearchIndexName)
								.type(ElasticSearchIndexType)
								.source(json);
					}

					@Override
					public void process(Film film, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(film));
					}
				}
		);

		filmDataStream.addSink(esSinkBuilder.build());

		env.execute("Running flink program.");
	}

	static class ParsePilots implements MapFunction<Tuple2<String, String>, Pilot>{
		@Override
		public Pilot map(Tuple2<String, String> s) throws Exception {
			JsonObject json = new JsonParser().parse(s.f1).getAsJsonObject();

			String name = json.get("name").getAsString();
			String mass = json.get("mass").getAsString();
			String hairColor = json.get("hair_color").getAsString();
			String height = json.get("height").getAsString();
			String gender = json.get("gender").getAsString();

			JsonArray filmsJson = json.get("films").getAsJsonArray();

			ArrayList<String> films = new ArrayList<>();
			for (int i =0; i < filmsJson.size(); i++){
				films.add(filmsJson.get(i).getAsString());
			}
			return new Pilot(gender, name, height, mass, hairColor, s.f0, films);
		}
	}


	static class ParseFilms implements MapFunction<Tuple2<String, String>, Film>{
		@Override
		public Film map(Tuple2<String, String> s) throws Exception {
			JsonObject json = new JsonParser().parse(s.f1).getAsJsonObject();

			String name = json.get("title").getAsString();
			String openingCrawl = json.get("opening_crawl").getAsString();
			String director = json.get("director").getAsString();
			String producer = json.get("producer").getAsString();
			String releaseDate = json.get("release_date").getAsString();

			return new Film(s.f0, name, openingCrawl, director, producer, releaseDate);
		}
	}

	static class FilterDuplicates extends RichFilterFunction<Tuple2<String, String>> {
		private transient HashSet<String> unique;
		// Did not have time to configure state backend. :(
		@Override
		public void open(Configuration parameters) throws Exception {
			this.unique = new HashSet<>();
		}

		@Override
		public boolean filter(Tuple2<String, String> s) throws Exception {
			if (this.unique.contains(s.f1)){
				return false;
			}else{
				this.unique.add(s.f1);
				return true;
			}
		}
	}

	static class FlatPilotUrls implements FlatMapFunction<StarShip, Tuple2<String, String>>{
		@Override
		public void flatMap(StarShip ship, Collector<Tuple2<String, String>> out) throws Exception {
			ship.getPilotsUrl().forEach((url) -> {
				out.collect(new Tuple2<String, String>(ship.name, url));
			});
		}
	}

	static class FlatFilmUrls implements FlatMapFunction<Pilot, Tuple2<String, String>>{
		@Override
		public void flatMap(Pilot pilot, Collector<Tuple2<String, String>> out) throws Exception {
			pilot.getFilms().forEach((url) -> {
				out.collect(new Tuple2<String, String>(pilot.name, url));
			});
		}
	}

	static class FilterPilots implements FilterFunction<StarShip>{
		@Override
		public boolean filter(StarShip s) throws Exception {
			return s.pilotsUrl.size() > 0;
		}
	}

	static class FilterFilms implements FilterFunction<Pilot>{
		@Override
		public boolean filter(Pilot s) throws Exception {
			return s.films.size() > 0;
		}
	}

	static class ParseStarShips implements MapFunction<String, StarShip> {
		public StarShip map(String value) {
			JsonObject json = new JsonParser().parse(value).getAsJsonObject();
			String name = json.get("name").getAsString();
			JsonArray pilotUrlsJson = json.get("pilots").getAsJsonArray();

			ArrayList<String> pilotUrls = new ArrayList<>();
			for (int i = 0; i < pilotUrlsJson.size(); i++){
				pilotUrls.add(pilotUrlsJson.get(i).getAsString());
			}
			return new StarShip(name, pilotUrls);
		}
	}

	static class hitUrlsAsync extends RichAsyncFunction<Tuple2<String, String>, Tuple2<String, String>> {
		@Override
		public void asyncInvoke(Tuple2<String, String> url, final ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
			CompletableFuture<HttpResponse<String>> future = Unirest.get(url.f1).asStringAsync();
			CompletableFuture.supplyAsync(new Supplier<String>() {
				@Override
				public String get() {
					try {
						return future.get().getBody();
					} catch (InterruptedException | ExecutionException e) {
						return "null";
					}
				}
			}).thenAccept( (String resp) -> {
				resultFuture.complete(Collections.singleton(new Tuple2<>(url.f0, resp)));
			});
		}
	}
}
