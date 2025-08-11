// static/js/components/catalog.js

// Нет import!
// Все функции в window

window.catalog = function () {
  return {
    /* ───────── состояние ───────── */
    products:   [],
    categories: [],
    loading:    true,

    page:       1,
    perPage:    20,
    totalPages: 1,

    /* H1-заголовок страницы */
    currentTitle: 'Каталог товаров',

    /* выбранные фильтры (меняются из filterModal) */
    filters: {
      price:    null,
      length:   null,
      width:    null,
      height:   null,
      ordering: '-price',
    },

    /* ───────── computed ───────── */
    get slug () {
      return window.location.pathname.split('/').filter(Boolean).pop() || '';
    },

    getAttr (product, name) {
      const a = product.attributes
        .find(x => x.attribute.toLowerCase() === name);
      return a ? a.value : null;
    },

    /* ───────── lifecycle ───────── */
    async init () {
      try {
        const catResp = await window.apiGet('/api/categories/');
        this.categories = Array.isArray(catResp)
          ? catResp
          : catResp.results ?? [];

        const cat = this.categories.find(c => c.slug === this.slug);
        if (cat) {
          this.currentTitle = cat.title;
          // document.title    = cat.title;
        }
      } catch (e) { console.error('Не удалось загрузить категории', e); }

      await this.fetchProducts();
    },

    /* ───────── helpers ───────── */
    buildParams () {
      const p = { page: this.page };
      if (this.slug) p['category__slug'] = this.slug;

      if (this.filters.price) {
        const [min, max] = this.filters.price.split('-');
        if (min) p['price_min'] = min;
        if (max) p['price_max'] = max;
      }

      const map = { length: 'length', width: 'width', height: 'height' };
      for (const k in map) {
        if (!this.filters[k]) continue;
        const [min, max] = this.filters[k].split('-');
        if (min) p[`${map[k]}_min`] = min;
        if (max) p[`${map[k]}_max`] = max;
      }

      if (this.filters.ordering) p.ordering = this.filters.ordering;
      return p;
    },

    async fetchProducts () {
      this.loading = true;
      try {
        const data = await window.apiGet('/api/products/', this.buildParams());
        this.products = data.results ?? data;
        this.totalPages = data.count
          ? Math.ceil(data.count / this.perPage)
          : 1;
      } finally { this.loading = false; }
    },

    async applyFilters (obj) {
      this.page = 1;
      this.filters = { ...this.filters, ...obj };
      await this.fetchProducts();
    },

    col (n) {
      return this.products.filter((_, i) => i % 3 === n);
    },

    nextPage () {
      if (this.page < this.totalPages) {
        this.page++;
        this.fetchProducts();
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }
    },
    prevPage () {
      if (this.page > 1) {
        this.page--;
        this.fetchProducts();
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }
    },
  };
};
