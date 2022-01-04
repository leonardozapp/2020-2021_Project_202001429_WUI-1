<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoRentabilidadeFundo $tipoRentabilidadeFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $tipoRentabilidadeFundo->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $tipoRentabilidadeFundo->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Tipo Rentabilidade Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoRentabilidadeFundos form content">
            <?= $this->Form->create($tipoRentabilidadeFundo) ?>
            <fieldset>
                <legend><?= __('Edit Tipo Rentabilidade Fundo') ?></legend>
                <?php
                    echo $this->Form->control('rentabilidade');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
