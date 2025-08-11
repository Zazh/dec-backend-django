// static/js/components/homeCategories.js

// Нет import!

window.homeCategories = function () {
  return {
    categories: [],
    loading: true,

    async init() {
      // DRF может вернуть либо массив, либо объект { results:[...] }
      const raw = await window.apiGet('/api/categories/', { "parent__isnull": true });
      this.categories = Array.isArray(raw) ? raw : raw.results ?? [];
      this.categories = this.categories.filter(cat => cat.parent_id === null);
      this.loading = false;
    },
  };
};
