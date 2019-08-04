package org.myorg.quickstart;

import java.security.PublicKey;
import java.util.ArrayList;

public class StarShip {
    public String name;
    public ArrayList<String> pilotsUrl;

    public StarShip(String name, ArrayList<String> pilotsUrl) {
        this.name = name;
        this.pilotsUrl = pilotsUrl;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public ArrayList<String> getPilotsUrl() {
        return pilotsUrl;
    }

    public void setPilotsUrl(ArrayList<String> pilotsUrl) {
        this.pilotsUrl = pilotsUrl;
    }

    @Override
    public String toString() {
        return name;
    }
}
