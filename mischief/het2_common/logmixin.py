import os
from het2_common.globals import DEPLOY_PATH

class LogMixin(object):
    """
    Needs:

        .time_base
        .tset_num

    Provides:

        .points_to_file
        .setup_log_file
        .close_log_file
    """
    def setup_log_file(self, filename):
        self.debug_file = open(
            os.path.join(DEPLOY_PATH, 'log/trajectory/%s' %filename), 'w')

    def _format(self, point):
        return ('  SET_TRAJ ... %s %s %s %s %s %s %s %s'
                %tuple([self.tset_num]+list(point)))

    def points_to_file(self, points):
        points_str = '\n'.join(self._format(point) for point in points)
        debug_msg = ('---\n'
                     'At index time: %s\n'
                     '%s') %(self.time_base.index_time_now(), points_str)
        self.debug_file.write(debug_msg + '\n')

    def close_log_file(self):
        self.debug_file.close()