package br.com.alura.ecommerce.common.kafka;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonSerializer<T> implements Serializer<T>{

	// Devolve um json, que é um serializador
	private final Gson gson = new GsonBuilder().create();

	@Override
	public byte[] serialize(String s, T object) {
		// Retorna um Json do Object, porem ele devolve uma String, é necessário transformar ele em bytes
		return gson.toJson(object).getBytes(); 
	}
	
}
