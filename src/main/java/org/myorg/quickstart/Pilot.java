package org.myorg.quickstart;

import java.util.ArrayList;

public class Pilot {
    public String gender;
    public String name;
    public String height;
    public String mass;
    public String hairColor;
    public String shipName;
    public ArrayList<String> films;

    public Pilot(String gender, String name, String height, String mass, String hairColor, String shipName, ArrayList<String> films) {
        this.gender = gender;
        this.name = name;
        this.height = height;
        this.mass = mass;
        this.hairColor = hairColor;
        this.shipName = shipName;
        this.films = films;
    }

    public ArrayList<String> getFilms() {
        return films;
    }

    public void setFilms(ArrayList<String> films) {
        this.films = films;
    }

    public String getShipName() {
        return shipName;
    }

    public void setShipName(String shipName) {
        this.shipName = shipName;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getMass() {
        return mass;
    }

    public void setMass(String mass) {
        this.mass = mass;
    }

    public String getHairColor() {
        return hairColor;
    }

    public void setHairColor(String hairColor) {
        this.hairColor = hairColor;
    }

    public String getName() {
        return name;
    }

    public Pilot(String name, String height) {
        this.name = name;
        this.height = height;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHeight() {
        return height;
    }

    public void setHeight(String height) {
        this.height = height;
    }

}
