#!/usr/bin/env python

import cleanup_marathon_orphaned_images


class TestCleanupMarathonOrphanedImages:
    def test(self):
        cleanup_marathon_orphaned_images.main()
