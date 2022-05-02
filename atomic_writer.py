import os


class AtomicWriter(object):
    def __init__(self,):
        pass

    def __enter__(self,
                  file: str,
                  mode='wb',
                  **kwargs):
        # preserve with statement compatibility
        self.file = file
        self.mode = mode

        self.fileIO = open(
            self.file,
            mode=self.mode,
            **kwargs)

        return self.fileIO

    def __exit__(self):
        try:
            self.fileIO.flush()
            # check if file is written completely on disk
            os.fsync(self.fileIO.fileno())
            self.fileIO.close()
        except Exception:
            try:
                # rollback method
                os.unlink(self.file)
            except Exception:
                pass

    def __call__(self, *args, **kwargs):
        return self.__enter__(*args, **kwargs)


atomic_writer = AtomicWriter()