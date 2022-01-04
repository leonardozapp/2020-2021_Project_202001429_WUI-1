<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\RetornoRiscoFundo $retornoRiscoFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $retornoRiscoFundo->cnpj_fundo_id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $retornoRiscoFundo->cnpj_fundo_id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Retorno Risco Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="retornoRiscoFundos form content">
            <?= $this->Form->create($retornoRiscoFundo) ?>
            <fieldset>
                <legend><?= __('Edit Retorno Risco Fundo') ?></legend>
                <?php
                    echo $this->Form->control('periodo_meses');
                    echo $this->Form->control('data_final');
                    echo $this->Form->control('rentab_media');
                    echo $this->Form->control('desvio_padrao');
                    echo $this->Form->control('num_valores');
                    echo $this->Form->control('rentab_min');
                    echo $this->Form->control('rentab_max');
                    echo $this->Form->control('meses_abaixo_bench');
                    echo $this->Form->control('meses_acima_bench');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
