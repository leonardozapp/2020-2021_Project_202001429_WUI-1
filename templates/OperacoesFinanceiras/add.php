<?php

/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\OperacoesFinanceira $operacoesFinanceira
 */
?>

<script>
    $('document').ready(function() {
        $('#busca-fundo').keyup(function() {
            var searchkey = $(this).val();
            searchFundos(searchkey);
        });

        function searchFundos(keyword) {
            var data = keyword;
            $.ajax({
                method: 'get',
                url: "<?php echo $this->Url->build(['controller' => 'Fundos', 'action' => 'Ajaxsearch']); ?>",
                data: {
                    keyword: data
                },

                success: function(response) {
                    $('.resultado_busca').html(response);
                }
            });
        };
    });
</script>

<div class="row">
    <div class="column-responsive column-100">
        <div class="operacoesFinanceiras form content">
            <?= $this->element('titleInfo', array('title' => __('Nova Operação Financeira da Carteira "{0}"', $carteirasInvestimentos->nome), 'h' => 2)); ?>
            <?= $this->Form->create($operacoesFinanceira) ?>
            <fieldset>
                <?php
                echo $this->Form->control('busca_fundo', ['label' => __('Nome ou CNPJ do fundo para busca'), 'type' => 'text']);
                ?>
                <div class="resultado_busca"></div>
                <?php
                echo $this->Form->control('cnpj_fundo_id', ['label' => __('Id do fundo escolhido'), 'type' => 'text']);
                echo $this->Form->control('distribuidor_fundo_id', ['options' => $distribuidorFundos, 'empty' => true]);
                echo $this->Form->control('tipo_operacoes_financeira_id', ['options' => $tipoOperacoesFinanceiras]);
                echo $this->Form->control('por_valor');
                echo $this->Form->control('valor_total');
                echo $this->Form->control('valor_cota');
                echo $this->Form->control('quantidade_cotas');
                echo $this->Form->control('data');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>