package org.myorg.quickstart;

import com.google.gson.*;
import kong.unirest.Unirest;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class SwapiSource extends RichSourceFunction<String> {
    private volatile boolean isProducing = true;
    private String url = "https://swapi.co/api/starships/";

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while(isProducing){

            String response = Unirest.get(this.url)
                    .asString()
                    .getBody();

            JsonObject respJson = new JsonParser().parse(response).getAsJsonObject();
            boolean isNextUrlNull = respJson.get("next").isJsonNull();
            JsonArray results = respJson.get("results").getAsJsonArray();

            Gson gson = new Gson();

            for (int i = 0; i < results.size(); i++) {
                JsonObject starShip = results.get(i).getAsJsonObject();
                JsonElement ship = gson.fromJson(starShip, JsonElement.class);
                sourceContext.collect(gson.toJson(ship));
            }

            if (isNextUrlNull){
                cancel();
            }else{
                this.url = respJson.get("next").getAsString();
            }
        }
    }

    @Override
    public void cancel() {
        isProducing = false;
    }
}
