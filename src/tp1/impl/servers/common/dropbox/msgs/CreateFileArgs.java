package tp1.impl.servers.common.dropbox.msgs;

public record CreateFileArgs(String path, String mode, boolean autorename, boolean mute, boolean strict_conflict) {
}
