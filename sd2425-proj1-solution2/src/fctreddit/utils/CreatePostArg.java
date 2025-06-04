package fctreddit.utils;

import fctreddit.api.Post;

public record CreatePostArg(
        String key,
        Post data
) {
}
