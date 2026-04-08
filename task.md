Необходимо написать скрипт C:\FullStack\Scrapy\scripts\prom_prosale_automation.py для автосортировки новых товаров в кампании ProSale в админке ПРОМа.

Перед выполнением прочитай скрипт C:\FullStack\Scrapy\scripts\prom_noindex_automation.py, который также бегает по админке ПРОМа для автоматизации - что-то пригодится для решения задания.
Реализовать можно по алгоритму:
1.Ищем и Кликаем в сайд баре по тегу

<div class="l-GwW fvQVX"><span class="_3Trjq IpUr5 SCVcX index__menuItemTitle--LrrQH" data-qaid="menu_item_title">Каталог ProSale</span></div>
откроется страница со всеми кампаниями просале

2. Ищем и Кликаем 1-ю по порядку кампанию просале
<td class="CommonTable__cell--c3DD4 CommonTable__cell_padding-l_20--DWz4Z CommonTable__cell_valign_middle--HMTRj"><a data-qaid="name" href="/cms/prosale/4219986">SECUR CPA</a><div class="M3v0L _1QkPI"><div class="MafxA WIR6H vSe5U"></div></div></td>

откроется страница с этой кампанией.

3. Ищем и кликаем тег
   <button type="button" data-qaid="add_product_link" class="I80Um XgilV _2FJck"><svg width="1em" height="1em" fill="none" viewBox="0 0 24 24" data-testid="SvgPlus" data-qaid="SvgPlus" focusable="false" aria-hidden="true" class="D8VyR sHErq" style="width: 18px; height: 18px;"><path fill="currentColor" fill-rule="evenodd" d="M19 11h-6V5a1 1 0 1 0-2 0v6H5a1 1 0 1 0 0 2h6v6a1 1 0 1 0 2 0v-6h6a1 1 0 1 0 0-2Z" clip-rule="evenodd"></path></svg>&nbsp;<span class="_3Trjq xrKUz">Додати товар або групу</span></button>

Откроется модалка

<div class="b-react-overlay__header">Додати товар або групу</div>

4. В модалке ищем и кликаем тег
<div data-qaid="select_dropdown" class="styles__labelDropdown--nIPV9"><span>Виберіть нотатку</span><svg class="b-smart-filter__dd-arrow" viewBox="0 0 64 64"><use xlink:href="#arrow-down"></use></svg></div>

Откроется меню

<div class="b-multiselect-creator__wrapper b-multiselect-creator__wrapper_without_arrow" data-qaid="add_tag_popup"><div class="b-multiselect-creator__search"><div class="b-search"><svg class="b-search__icon-search b-svg b-svg_size_20 b-svg_fill_black-600"><use xlink:href="/image/svg_sprites/cms.svg?rev#svg-search"></use></svg><input class="b-search__input" data-qaid="search_tag_input" type="text" placeholder="Пошук" autocomplete="off" maxlength="255" value=""></div></div><ul class="b-multiselect-creator__list b-multiselect-creator__list_140"><li class="b-multiselect-creator__list-item b-multiselect-creator__list-item_state_hover"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="228317" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">V</span></span></div></div></label></li><li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="228383" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">S</span></span></div></div></label></li><li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="12872692" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">W</span></span></div></div></label></li><li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="104305394" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">ROZ</span></span></div></div></label></li><li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="120615091" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">VMAX</span></span></div></div></label></li><li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="120615092" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">Vmin</span></span></div></div></label></li><li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="125965023" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">Sprom</span></span></div></div></label></li><li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="126612716" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">Wprom</span></span></div></div></label></li></ul><div class="b-multiselect-creator__mark b-hidden"><svg class="b-multiselect-creator__mark-icon b-svg b-svg_size_12 b-svg_fill_blue"><use xlink:href="/image/svg_sprites/cms.svg?rev#svg-plus"></use></svg><span class="b-pseudo-link" data-qaid="new_tag_link">Створити мітку </span></div><div class="b-multiselect-creator__action"><button type="button" disabled="" data-qaid="save_btn" class="I80Um vFL5E DuFM2">Додати</button></div></div>

Необходимо найти и поставить чек-бокс на

<li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="125965023" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">Sprom</span></span></div></div></label></li>

В этом же меню найти и кликнуть
<button type="button" data-qaid="save_btn" class="I80Um vFL5E DuFM2">Додати</button>

В модалке появятся товары.

5. Найти в модалке и поставить чек-бокс на
   <label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="select_all"><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq">Вибрати все</span></div></div></label>

   Если тега Вибрати все нет (товаров нет для добавления) , то найти в модалке и кликнуть
   <span class="b-react-overlay__close-button qa-close-button" data-qaid="close_btn">×</span>, модалка закроется и дальше пункт 7

6. Найти в модалке и кликнуть
   <button type="button" data-qaid="add_product_btn" class="I80Um DuFM2">Додати до кампанії</button>
   Модалка закроется.

7. Найти на странице кампании и кликнуть
<div class="l-GwW" style="flex-shrink: 0;"><a href="/cms/prosale"><svg width="1em" height="1em" fill="none" viewBox="0 0 24 24" data-testid="SvgArrowBack" data-qaid="SvgArrowBack" focusable="false" aria-hidden="true" class="D8VyR _6WmlM" style="width: 24px; height: 24px;"><path fill="currentColor" d="M7.4 10.987h11.175c.283 0 .52.096.713.288.191.191.287.429.287.712 0 .284-.096.521-.287.713a.968.968 0 0 1-.713.287H7.4l4.9 4.9c.2.2.296.433.287.7-.008.267-.112.5-.312.7-.2.184-.433.28-.7.288a.916.916 0 0 1-.7-.288l-6.6-6.6a.877.877 0 0 1-.213-.325A1.107 1.107 0 0 1 4 11.987c0-.133.02-.258.063-.375a.877.877 0 0 1 .212-.325l6.6-6.6a.933.933 0 0 1 .688-.275c.274 0 .512.092.712.275.2.2.3.438.3.713 0 .275-.1.512-.3.712L7.4 10.987Z"></path></svg></a></div>

выходим на страницу всех кампаний .

8. Ищем кампании и по тому же алгоритму обрабатываем еще кампании, НО со своими тегами из пункта 4:
   кампания VIATEC MAX CPA
   <li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="125965023" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">VMAX</span></span></div></div></label></li>

   кампания VIATEC MIN CPA
   <li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="125965023" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">Vmin</span></span></div></div></label></li>

   кампания SECUR CPC
   <li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="125965023" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">Sprom</span></span></div></div></label></li>

кампания VIATEC MAX CPC

   <li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="125965023" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">VMAX</span></span></div></div></label></li>

кампания VIATEC MIN CPC

   <li class="b-multiselect-creator__list-item"><label class="M3v0L DKc7M"><div class="MafxA WIR6H _0NLAD"><div class="l-GwW"><div class="QNYqa"><input class="Yi8X2" type="checkbox" data-qaid="item_chbx" id="125965023" readonly=""><div class="fLkiL"></div></div></div><div class="l-GwW fvQVX"><span class="_3Trjq"><span data-qaid="add_tag_name">Vmin</span></span></div></div></label></li>

9. Этот скрипт также надо добавить в C:\FullStack\Scrapy\.github\workflows\pipeline.yml аналогично как C:\FullStack\Scrapy\scripts\prom_noindex_automation.py с той же задержкой и выполнением параллельно или
   последовательно - тут смотри сам как оптимальнее, чтобы пайплайн не завис.

10. Критично: если надо что-то уточнить по логике и написанию кода , то спрашивай до написания кода.
