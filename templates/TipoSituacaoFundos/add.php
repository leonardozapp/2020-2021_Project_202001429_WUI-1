<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoSituacaoFundo $tipoSituacaoFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('List Tipo Situacao Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoSituacaoFundos form content">
            <?= $this->Form->create($tipoSituacaoFundo) ?>
            <fieldset>
                <legend><?= __('Add Tipo Situacao Fundo') ?></legend>
                <?php
                    echo $this->Form->control('situacao');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
