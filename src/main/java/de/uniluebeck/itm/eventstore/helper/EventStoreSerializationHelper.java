package de.uniluebeck.itm.eventstore.helper;


import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class EventStoreSerializationHelper<T> {
    private static Logger log = LoggerFactory.
            getLogger(EventStoreSerializationHelper.class);
    public static final String SERIALIZER_MAP_FILE_EXTENSION = ".mapping";
    private Map<Class<T>, Function<T, byte[]>> serializers;
    private Map<Byte, Function<byte[], T>> deserializers;
    private BiMap<Class<T>, Byte> mapping;
    private final String mappingFileBasePath;

    public EventStoreSerializationHelper(final String mappingFileBasePath, final Map<Class<T>, Function<T, byte[]>> serializers,
                                         Map<Class<T>, Function<byte[], T>> deserializers) throws FileNotFoundException, ClassNotFoundException {
        this.mappingFileBasePath = mappingFileBasePath;
        buildMaps(serializers, deserializers);
    }


    public byte[] serialize(T object) throws NotSerializableException {
        @SuppressWarnings("unchecked") Class<T> c = (Class<T>) object.getClass();
        return serialize(object, c);
    }

    public byte[] serialize(T object, final Class<T> type) throws NotSerializableException {
        try {
            Function<T, byte[]> serializer = serializers.get(type);
            byte[] serialized = serializer.apply(object);
            byte typeByte = mapping.get(type);
            byte[] finalSerialization = new byte[serialized.length+1];
            finalSerialization[0] = typeByte;
            System.arraycopy(serialized, 0, finalSerialization, 1, serialized.length);
            return finalSerialization;
        } catch (NullPointerException e) {
            throw new NotSerializableException("Can't find a serializer for type " + type.getName());
        }
    }

    public T deserialize(byte[] serialization) throws IllegalArgumentException {
        if (serialization == null || serialization.length == 0) {
            throw new IllegalArgumentException("Can't deserialize empty byte array");
        }

        Function<byte[], T> deserializer = deserializers.get(serialization[0]);
        if (deserializer == null) {
            throw new IllegalArgumentException("The provided byte array is invalid. No matching serializer found!");
        }
        byte[] event = new byte[serialization.length-1];
        System.arraycopy(serialization,1,event,0,serialization.length-1);


        return deserializer.apply(event);
    }

    private void buildMaps(Map<Class<T>, Function<T, byte[]>> serializers,
                           Map<Class<T>, Function<byte[], T>> externalDeserializers) throws ClassNotFoundException, IllegalArgumentException, FileNotFoundException {


        HashMap<String, Byte> serializerMapping = loadPersistedSerializerMapping();
        mapping = HashBiMap.create();
        Byte maxByte = Byte.MIN_VALUE;
        this.serializers = serializers;
        this.deserializers = new HashMap<Byte, Function<byte[], T>>();

        // Build mapping for existing types
        List<String> notFoundClasses = new ArrayList<String>();
        for (Map.Entry<String, Byte> entry : serializerMapping.entrySet()) {
            Byte id = entry.getValue();
            String className = entry.getKey();
            try {
                Class clazz = Class.forName(className);
                mapping.put(clazz, id);
                if (id > maxByte) {
                    maxByte = id;
                }
            } catch (ClassNotFoundException e) {
                notFoundClasses.add(className);
            }
        }

        if (notFoundClasses.size() > 0) {
            throw new ClassNotFoundException("Unknown classes in event store mapping file. Check the mapping file and fix the class names if appropriate.\nUnknown Classes: " + notFoundClasses);
        }


        byte b = maxByte;
        if (serializers.size() > 0) {
            if (b < Byte.MAX_VALUE - serializers.size()) {
                b = (byte) (b + 1);
                for (Map.Entry<Class<T>, Function<byte[], T>> entry : externalDeserializers.entrySet()) {
                    String className = entry.getKey().getName();
                    if (!serializerMapping.containsKey(className)) {
                        serializerMapping.put(className, b);
                        mapping.put(entry.getKey(), b);
                        this.deserializers.put(b, entry.getValue());
                        b++;
                    } else {
                        this.deserializers.put(serializerMapping.get(className), entry.getValue());
                    }
                }


            } else {
                throw new IllegalArgumentException("Can't add that many serializers and deserializers!");
            }
        }

        if (!storeSerializerMapping(serializerMapping)) {
            throw new FileNotFoundException("Event Store can't create mapping file or file is not writable!: " + (mappingFileBasePath + SERIALIZER_MAP_FILE_EXTENSION));
        }
    }


    private HashMap<String, Byte> loadPersistedSerializerMapping() throws FileNotFoundException {
        File serializerMappingFile = new File(mappingFileBasePath + SERIALIZER_MAP_FILE_EXTENSION);
        HashMap<String, Byte> serializerMapping = new HashMap<String, Byte>();
        if (serializerMappingFile.exists()) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(serializerMappingFile));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] components = line.split(",");
                    if (components.length != 2) {
                        log.warn("Invalid line in mapping file: {}", line);
                        continue;
                    }
                    serializerMapping.put(components[0], Byte.parseByte(components[1]));
                }
            } catch (FileNotFoundException e) {
                log.error("Can't find mapping file {}.", serializerMappingFile.getAbsolutePath());
                throw new FileNotFoundException("Event Store can't find mapping file " + serializerMappingFile.getAbsolutePath());
            } catch (IOException e) {
                log.error("Failed to read line from CSV mapping file.", e);
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                    }
                }
            }

        }

        return serializerMapping;
    }

    private boolean storeSerializerMapping(HashMap<String, Byte> serializerMapping) {
        File serializerMappingFile = new File(mappingFileBasePath + SERIALIZER_MAP_FILE_EXTENSION);
        if (!serializerMappingFile.exists()) {
            try {
                serializerMappingFile.createNewFile();
            } catch (IOException e) {
                log.error("Can't create serializer mapping file.", e);
                return false;
            }
        }
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(serializerMappingFile));
            Iterator<Map.Entry<String, Byte>> it = serializerMapping.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Byte> entry = it.next();
                bw.write(entry.getKey() + "," + entry.getValue());
                if (it.hasNext()) {
                    bw.write("\n");
                }
            }
        } catch (IOException e) {
            log.error("Can't create file writer for mapping file.", e);
            return false;
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                }
            }
        }

        return true;
    }
}
