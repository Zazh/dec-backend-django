// static/js/components/searchModal.js

// Нет import!
// Все функции и переменные внутри файла

const LS_KEY      = 'search-history';
const MAX_HISTORY = 6;
const DEBOUNCE_MS = 400;

window.searchModal = function () {
  return {
    /* ─── состояние ─── */
    q:        '',
    results:  [],
    history:  [],
    state:    'empty',           // empty | typing | history | results
    loading:  false,
    debounceId: null,

    /* ─── computed ─── */
    get headerText () {
      if (this.loading)              return 'Идёт поиск…';
      if (this.state === 'results')
        return this.results.length ? 'Результаты поиска'
                                   : 'Товары не найдены';
      if (this.state === 'history')  return 'История поиска';
      return '';
    },
    get noResults () {
      return !this.loading && this.state === 'results' && this.results.length === 0;
    },

    /* ─── lifecycle ─── */
    init () {
      try {
        this.history = (JSON.parse(localStorage.getItem(LS_KEY)) || [])
                         .filter(h => h && h.id);
      } catch { this.history = []; }

      this.state = this.history.length ? 'history' : 'empty';

      // фокус после появления окна
      requestAnimationFrame(() => this.$refs.searchInput?.focus());
    },

    /* ─── ввод ─── */
    onType () {
      clearTimeout(this.debounceId);

      const txt = this.q.trim();

      // если короче 3-х → чистим результаты и возвращаем history/empty
      if (txt.length < 3) {
        this.results = [];
        this.state   = this.history.length ? 'history' : 'empty';
        return;
      }

      this.state = 'typing';
      this.debounceId = setTimeout(() => this.doSearch(), DEBOUNCE_MS);
    },

    async doSearch () {
      const query = this.q.trim();
      if (query.length < 3) return;

      this.loading = true;
      try {
        const data = await window.apiGet('/api/products/', {
          search:    query,
          page_size: 8
        });

        this.results = data.results ?? data;
        this.state   = 'results';
      } finally {
        this.loading = false;
      }
    },

    /* ─── клики ─── */
    openItem (product) {
      if (!product?.id) return;

      this.history = [
        product,
        ...this.history.filter(p => p.id !== product.id)
      ].slice(0, MAX_HISTORY);

      localStorage.setItem(LS_KEY, JSON.stringify(this.history));
      window.location.href = `/product/${product.slug}/`;
    },

    clearHistory () {
      this.history = [];
      localStorage.removeItem(LS_KEY);
      this.state = 'empty';
    }
  };
};
