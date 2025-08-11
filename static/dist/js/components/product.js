window.product = function () {
  return {
    slug:   window.location.pathname.split('/').filter(Boolean).pop(),
    item:   { category: { slug: '', title: '' }, images: [], attributes: [] },
    related: [],
    currentImage: {},
    loading: true,

    attrValues (name) {
      return (this.item.attributes || [])
        .filter(a => a.attribute.toLowerCase() === name.toLowerCase())
        .map   (a => a.value);
    },
    get heightText  ()  { return this.attrValues('высота').join(', ');  },
    get widthText   ()  { return this.attrValues('длина').join(', '); },
    get sizeText    ()  { return this.attrValues('ширина').join(', '); },

    async init () {
      try {
        this.item = await window.apiGet(`/api/products/${this.slug}/`);
        this.currentImage =
          this.item.images.find(i => i.is_main) || this.item.images[0] || {};
        await this.loadRelated();
      } catch (e) {
        console.error('Не удалось загрузить товар', e);
      } finally {
        this.loading = false;
      }
    },

    async loadRelated () {
      const catSlug = this.item.category?.slug;
      if (!catSlug) return;
      try {
        const data = await window.apiGet('/api/products/', {
          category__slug: catSlug,
          page_size: 100
        });
        const products = Array.isArray(data) ? data : (data.results || []);
        if (products.length <= 10) {
          this.related = products.filter(p => p.id !== this.item.id);
        } else {
          const filteredProducts = products.filter(p => p.id !== this.item.id);
          const shuffled = filteredProducts.sort(() => 0.5 - Math.random());
          this.related = shuffled.slice(0, 10);
        }
      } catch (e) {
        console.error('Ошибка при загрузке похожих товаров:', e);
        this.related = [];
      }
    },

    formatPrice(price) {
      if (price === undefined || price === null) return '—';
      return `${Number(price).toLocaleString('ru-RU')} ₸`;
    },
  };
};
