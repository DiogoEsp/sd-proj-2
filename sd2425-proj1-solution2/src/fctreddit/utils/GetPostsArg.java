package fctreddit.utils;

public record GetPostsArg(
        String key,
        long timestamp,
        String sortOrder
) {
}
