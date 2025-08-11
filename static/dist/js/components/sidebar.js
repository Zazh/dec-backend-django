// static/js/components/sidebar.js

// Нет import!
// Всё в window

window.sidebar = function () {
  return {
    categories: [],

    get activeSlug () {
      return window.location.pathname.split('/').filter(Boolean).pop() || '';
    },

    async init () {
      try {
        const data = await window.apiGet('/api/categories/');

        // DRF-пагинация: берём data.results, иначе сам data
        const list = Array.isArray(data) ? data : data.results ?? [];

        // фильтруем null и дубликаты id
        const seen = new Set();
        this.categories = list.filter(c => {
          if (!c || c.id == null || seen.has(c.id)) return false;
          seen.add(c.id);
          return true;
        });

      } catch (err) {
        console.error('Не удалось загрузить категории', err);
        this.categories = [];
      }
    },
  };
};
