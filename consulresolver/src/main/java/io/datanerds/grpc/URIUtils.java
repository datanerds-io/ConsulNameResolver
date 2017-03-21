package io.datanerds.grpc;

import com.google.common.base.Strings;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public class URIUtils {

    public static Map<String, List<String>> splitQuery(URI url) {
        if (Strings.isNullOrEmpty(url.getQuery())) {
            return Collections.emptyMap();
        }

        return stream(url.getQuery().split("&"))
                .map(URIUtils::splitQueryParameter)
                .filter(x -> !Strings.isNullOrEmpty(x.getValue()))
                .filter(x -> "dc".equals(x.getKey()) || "tag".equals(x.getKey()))
                .collect(Collectors.groupingBy(AbstractMap.SimpleImmutableEntry::getKey,
                        LinkedHashMap::new, mapping(Map.Entry::getValue, toList())
                ));
    }

    private static AbstractMap.SimpleImmutableEntry<String, String> splitQueryParameter(String it) {
        int idx = it.indexOf("=");
        String key = idx > 0 ? it.substring(0, idx) : it;
        String value = idx > 0 && it.length() > idx + 1 ? it.substring(idx + 1) : null;
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }
}
