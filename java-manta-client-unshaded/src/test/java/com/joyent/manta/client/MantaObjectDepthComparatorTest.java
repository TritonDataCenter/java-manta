/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.http.MantaContentTypes;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.joyent.manta.client.MantaClient.SEPARATOR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
public class MantaObjectDepthComparatorTest {
    public void verifyOrdering() {
        List<MantaObject> objects = new ArrayList<>();
        List<MantaObject> dirs = dirObjects(12);

        for (MantaObject dir : dirs) {
            objects.add(dir);
            objects.addAll(fileObjects(dir, 3));
        }

        Collections.shuffle(objects);

        Collections.sort(objects, MantaObjectDepthComparator.INSTANCE);

//        System.out.println(StringUtils.join(objects, "\n"));

        String[] actual = objects.stream()
                .filter(MantaObject::isDirectory)
                .map(MantaObject::toString)
                .toArray(String[]::new);

        String[] expected = new String[] {
                "[F] /user/stor/ggg/ggg/ggg/ggg/dddd.json",
                "[F] /user/stor/ggg/ggg/ggg/ggg/dddd.json",
                "[F] /user/stor/ggg/ggg/ggg/ggg/dddd.json",
                "[F] /user/stor/ggg/ggg/ggg/dddd.json",
                "[D] /user/stor/ggg/ggg/ggg/ggg",
                "[F] /user/stor/ggg/ggg/ggg/dddd.json",
                "[F] /user/stor/ggg/ggg/ggg/dddd.json",
                "[F] /user/stor/ggg/ggg/dddd.json",
                "[F] /user/stor/ggg/ggg/dddd.json",
                "[D] /user/stor/ggg/ggg/ggg",
                "[F] /user/stor/ggg/ggg/dddd.json",
                "[F] /user/stor/ggg/dddd.json",
                "[F] /user/stor/ggg/dddd.json",
                "[F] /user/stor/ggg/dddd.json",
                "[D] /user/stor/ggg/ggg",
                "[D] /user/stor/ggg"
        };

        Assert.assertEquals(actual, expected, "Objects were not sorted "
                + "in their expected order. Actual sorting:\n" +
                StringUtils.join(objects, "\n"));
    }

    private static List<MantaObject> fileObjects(final MantaObject dirObject, final int number) {
        List<MantaObject> objects = new ArrayList<>();

        for (int i = 0; i < number; i++) {
            final MantaObject object = mock(MantaObject.class);
            String path = dirObject.getPath() + SEPARATOR
                    + StringUtils.repeat((char)(97 + number + i), 4) + ".json";
            when(object.getPath()).thenReturn(path);
            when(object.getType()).thenReturn(MantaObject.MANTA_OBJECT_TYPE_OBJECT);
            when(object.getContentType()).thenReturn(ContentType.APPLICATION_JSON.toString());
            when(object.toString()).thenReturn("[F] " + path);
            objects.add(object);
        }

        return objects;
    }

    private static List<MantaObject> dirObjects(final int depth) {
        List<MantaObject> objects = new ArrayList<>();

        StringBuilder path = new StringBuilder(SEPARATOR);
        path.append("user").append(SEPARATOR);
        path.append("stor");

        for (int i = 3; i <= depth; i++) {
            path.append(SEPARATOR).append(StringUtils.repeat((char)(97 + depth + i), 3));

            final MantaObject object = mock(MantaObject.class);
            when(object.getType()).thenReturn(MantaObject.MANTA_OBJECT_TYPE_DIRECTORY);
            when(object.getContentType()).thenReturn(MantaContentTypes.DIRECTORY_LIST.toString());
            when(object.getPath()).thenReturn(path.toString());
            when(object.toString()).thenReturn("[D] " + path.toString());
            objects.add(object);
        }

        return objects;
    }

}
