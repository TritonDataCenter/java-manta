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

import java.nio.file.Paths;
import java.util.*;

import static com.joyent.manta.client.MantaClient.SEPARATOR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
public class MantaObjectDepthComparatorTest {

    public void verifyOrderingWithSmallDataSet() {
        List<MantaObject> objects = new ArrayList<>();
        List<MantaObject> dirs = dirObjects(12);

        for (MantaObject dir : dirs) {
            objects.add(dir);
            objects.addAll(fileObjects(dir, 3));
        }

        Collections.shuffle(objects);

        objects.sort(MantaObjectDepthComparator.INSTANCE);

        assertOrdering(objects);
    }

    public void verifyOrderingWithSmallDataSetAndEmptyDirectories() {
        List<MantaObject> objects = new ArrayList<>();
        List<MantaObject> dirs = dirObjects(12);

        for (MantaObject dir : dirs) {
            objects.add(dir);
            objects.addAll(fileObjects(dir, 3));
            objects.add(mockDirectory(dir.getPath() + MantaClient.SEPARATOR + "empty-dir"));
        }

        Collections.shuffle(objects);

        objects.sort(MantaObjectDepthComparator.INSTANCE);

        assertOrdering(objects);
    }

    public void verifyOrderingWithMoreSubdirectoriesDataSet() {
        List<MantaObject> objects = new ArrayList<>();
        List<MantaObject> dirs = dirObjects(29);

        for (MantaObject dir : dirs) {
            objects.add(dir);
            objects.addAll(fileObjects(dir, 24));

            MantaObject subdir = mockDirectory(dir.getPath()
                    + MantaClient.SEPARATOR + "subdir");
            objects.add(subdir);
            objects.addAll(fileObjects(subdir, 3));
        }

        Collections.shuffle(objects);

        objects.sort(MantaObjectDepthComparator.INSTANCE);

        assertOrdering(objects);
    }

    private static void assertOrdering(final List<MantaObject> sorted) {
        Set<String> parentDirs = new LinkedHashSet<>();
        int index = 0;

        for (MantaObject obj : sorted) {
            if (obj.isDirectory()) {
                String parentDir = Paths.get(obj.getPath()).getParent().toString();

                Assert.assertFalse(parentDirs.contains(parentDir),
                        "The parent of this directory was encountered before "
                            + "this directory [index=" + index + "].");

                parentDirs.remove(obj.getPath());
            } else {
                String fileParentDir = Paths.get(obj.getPath()).getParent().toString();

                Assert.assertFalse(parentDirs.contains(fileParentDir),
                        "Parent directory path was returned before file path. "
                                + "Index [" + index + "] was out of order. "
                                + "Actual sorting:\n" +
                                StringUtils.join(sorted, "\n"));
            }

            index++;
        }
    }

    private static List<MantaObject> fileObjects(final MantaObject dirObject, final int number) {
        List<MantaObject> objects = new ArrayList<>();

        for (int i = 0; i < number; i++) {
            final MantaObject object = mock(MantaObject.class);
            final int segmentChar = (number + i) % 26;
            String path = dirObject.getPath() + SEPARATOR
                    + StringUtils.repeat(((char)(97 + segmentChar) ), 4) + ".json";
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
            final int segmentChar = (depth + i) % 26;
            path.append(SEPARATOR).append(StringUtils.repeat(((char)(97 + segmentChar) ), 3));
            MantaObject object = mockDirectory(path);
            objects.add(object);
        }

        return objects;
    }

    private static MantaObject mockDirectory(final Object path) {
        final MantaObject object = mock(MantaObject.class);
        when(object.getType()).thenReturn(MantaObject.MANTA_OBJECT_TYPE_DIRECTORY);
        when(object.getContentType()).thenReturn(MantaContentTypes.DIRECTORY_LIST.toString());
        when(object.getPath()).thenReturn(path.toString());
        when(object.toString()).thenReturn("[D] " + path.toString());

        return object;
    }
}
