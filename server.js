// Добавить в server.js рядом с /pre-registry/approve, перед обработчиком 404.
// Использует существующие pool и normalizeLogin_.
// Проверка login повторяет текущую схему авторизации этого сервера.
app.post('/pre-registry/reject', async (req, res) => {
  try {
    const actor = normalizeLogin_(req.body?.login);
    const itemId = Number(req.body?.item_id);
    if (actor !== 'k_ermek') {
      return res.status(403).json({ success: false, error: 'ONLY_K_ERMEK' });
    }
    if (!Number.isSafeInteger(itemId) || itemId <= 0) {
      return res.status(400).json({ success: false, error: 'INVALID_ITEM_ID' });
    }
    // Условие UPDATE проверяется атомарно: утверждённую или распределённую
    // запись нельзя отклонить даже при устаревших данных на странице.
    const result = await pool.query(`
      UPDATE public.pre_registry_items
      SET approval_status = 'Отклонено',
          approved_amount = 0,
          approved_by = NULL,
          approved_at = NULL,
          updated_at = NOW()
      WHERE id = $1
        AND approval_status = 'Ожидает'
        AND distributed_at IS NULL
        AND COALESCE(distributed_by, '') = ''
      RETURNING *
    `, [itemId]);
    if (!result.rows.length) {
      return res.status(409).json({
        success: false,
        error: 'ITEM_NOT_PENDING',
        message: 'Запись не найдена либо уже обработана. Обновите ДоРеестр.'
      });
    }
    return res.json({ success: true, row: result.rows[0] });
  } catch (e) {
    console.error('PRE REGISTRY REJECT ERROR:', e);
    return res.status(500).json({ success: false, error: e.message });
  }
});
