package de.alkern.infofusion.eval.util;

public class FusionQueries {

    public final static String SIMPLE_MUSICBRAINZ_QUERY = "" +
            "CREATE MERGING\n" +
            "LET song\n" +
            "  title = property(\n" +
            "    (MusicSource.Song.name,\n" +
            "    Trackz.Track.title,\n" +
            "    MusicDB.Song.title,\n" +
            "    SongDB.Song.song_name,\n" +
            "    SongArchive.Song.song_name),\n" +
            "    textual:lcs)\n";

    public final static String MUSICBRAINZ_QUERY = "" +
            "CREATE MERGING\n" +
            "LET song\n" +
            "  title = property(\n" +
            "    (MusicSource.Song.name,\n" +
            "    Trackz.Track.title,\n" +
            "    MusicDB.Song.title,\n" +
            "    SongDB.Song.song_name,\n" +
            "    SongArchive.Song.song_name),\n" +
            "    textual:lcs),\n" +
            "  artist = property(\n" +
            "    (MusicSource.Song.artist,\n" +
            "    Trackz.Track.artist,\n" +
            "    MusicDB.Song.interpreter,\n" +
            "    SongDB.Song.interpreter,\n" +
            "    SongArchive.Song.artist),\n" +
            "    textual:lcs),\n" +
            "  album = majority(album),\n" +
            "  number = majority(number),\n" +
            "  year = majority(year),\n" +
            "  language = majority(language),\n" +
            "  length = priority(length, (" +
            "    Trackz, MusicDB, MusicSource, SongArchive, SongDB\n" +
            "  )),\n" +
            "  sources = retain(type, [|])\n";
}
