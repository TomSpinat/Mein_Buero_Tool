from PyQt6.QtCore import QObject, QTimer, pyqtSignal


class SmoothProgressController(QObject):
  progress_changed = pyqtSignal(float)
  indeterminate_changed = pyqtSignal(bool)
  visibility_changed = pyqtSignal(bool)
  phase_text_changed = pyqtSignal(str)
  settled = pyqtSignal(float)

  def __init__(self, parent=None, tick_ms=16):
    super().__init__(parent)
    self.target_progress = 0.0
    self.displayed_progress = 0.0
    self._indeterminate = False
    self._visible = False
    self._phase_text = ""
    self._pending_hide_delay_ms = None
    self._generation = 0
    self._tick_timer = QTimer(self)
    self._tick_timer.setInterval(max(12, int(tick_ms or 16)))
    self._tick_timer.timeout.connect(self._on_tick)

  def reset(self, progress=0.0, visible=False, phase_text=""):
    self._generation += 1
    value = self._clamp_progress(progress, allow_complete=True)
    self.target_progress = value
    self.displayed_progress = value
    self._pending_hide_delay_ms = None
    self._set_indeterminate(False)
    self._set_phase_text(phase_text)
    self._tick_timer.stop()
    self._emit_progress()
    self._set_visible(visible)
    if visible and not self._indeterminate:
      self.settled.emit(self.displayed_progress)

  def show_indeterminate(self, phase_text=None, visible=True):
    self._generation += 1
    self._pending_hide_delay_ms = None
    self._set_phase_text(phase_text)
    self._set_visible(visible)
    self._set_indeterminate(True)
    self._tick_timer.stop()

  def set_target_progress(self, progress, phase_text=None, visible=True, allow_complete=False):
    self._generation += 1
    clamped = self._clamp_progress(progress, allow_complete=allow_complete)
    self.target_progress = max(self.target_progress, clamped)
    self._pending_hide_delay_ms = None
    self._set_phase_text(phase_text)
    self._set_visible(visible)
    self._set_indeterminate(False)
    self._ensure_running()

  def finish(self, phase_text=None, hide_after_ms=180):
    self._pending_hide_delay_ms = max(0, int(hide_after_ms or 0))
    self.set_target_progress(1.0, phase_text=phase_text, visible=True, allow_complete=True)

  def set_phase_text(self, phase_text):
    self._set_phase_text(phase_text)

  def hide(self, clear_phase_text=False, reset_progress=True):
    self._generation += 1
    self._pending_hide_delay_ms = None
    self._tick_timer.stop()
    self._set_visible(False)
    self._set_indeterminate(False)
    if reset_progress:
      self.target_progress = 0.0
      self.displayed_progress = 0.0
      self._emit_progress()
    if clear_phase_text:
      self._set_phase_text("")

  def _ensure_running(self):
    if not self._visible or self._indeterminate:
      return
    if abs(self.target_progress - self.displayed_progress) <= 0.0005:
      self.displayed_progress = self.target_progress
      self._emit_progress()
      self._on_settled()
      return
    if not self._tick_timer.isActive():
      self._tick_timer.start()

  def _on_tick(self):
    if self._indeterminate or not self._visible:
      self._tick_timer.stop()
      return

    delta = self.target_progress - self.displayed_progress
    if delta <= 0.0005:
      self.displayed_progress = self.target_progress
      self._emit_progress()
      self._tick_timer.stop()
      self._on_settled()
      return

    # Grobe Spruenge laufen zuegig an, kleine Abstaende weich aus.
    step = max(0.0025, min(0.045, (delta * 0.22) + 0.002))
    if delta < 0.025:
      step = min(delta, max(0.0012, delta * 0.45))
    self.displayed_progress = min(self.target_progress, self.displayed_progress + step)
    self._emit_progress()

  def _on_settled(self):
    self.settled.emit(self.displayed_progress)
    if self._pending_hide_delay_ms is not None:
      delay_ms = self._pending_hide_delay_ms
      self._pending_hide_delay_ms = None
      generation = self._generation
      QTimer.singleShot(delay_ms, lambda gen=generation: self._hide_if_current(gen))

  def _emit_progress(self):
    self.progress_changed.emit(float(self.displayed_progress))

  def _hide_if_current(self, generation):
    if int(generation or 0) != self._generation:
      return
    self.hide()

  def _set_indeterminate(self, enabled):
    enabled = bool(enabled)
    if self._indeterminate == enabled:
      return
    self._indeterminate = enabled
    self.indeterminate_changed.emit(enabled)

  def _set_visible(self, visible):
    visible = bool(visible)
    if self._visible == visible:
      return
    self._visible = visible
    self.visibility_changed.emit(visible)

  def _set_phase_text(self, text):
    if text is None:
      return
    text = str(text or "").strip()
    if self._phase_text == text:
      return
    self._phase_text = text
    self.phase_text_changed.emit(text)

  @staticmethod
  def _clamp_progress(progress, allow_complete=False):
    value = max(0.0, min(float(progress or 0.0), 1.0))
    if not allow_complete:
      value = min(value, 0.985)
    return value
